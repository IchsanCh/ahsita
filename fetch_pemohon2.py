import requests
import mysql.connector
import hashlib
import json
import time
from datetime import datetime, timezone, timedelta
import re  # untuk validasi nomor

# ========================
# Konfigurasi & Utilitas
# ========================

def log(msg: str):
    """Logger sederhana dengan timestamp dan flush biar langsung tampil di terminal."""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)

# Timezone WIB (Asia/Jakarta)
try:
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
    try:
        WIB = ZoneInfo("Asia/Jakarta")
    except ZoneInfoNotFoundError:
        WIB = timezone(timedelta(hours=7))
except ImportError:
    WIB = timezone(timedelta(hours=7))

# --- DATABASE CONFIGS ---
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'database': 'notifsan'
}

FONNTE_URL = "https://api.fonnte.com/send"
USER_API = "http://notifsan.test/api/v1/ichsan"
API_TOKEN = "rahasia-token-aku-123"

# Timeout untuk requests: (connect, read)
REQUEST_TIMEOUT = (5, 45)


# ========================
# Helper business logic
# ========================

def compute_hash(item: dict) -> str:
    """Hash payload untuk deteksi perubahan."""
    raw = json.dumps(item or {}, sort_keys=True, ensure_ascii=False)
    return hashlib.md5(raw.encode('utf-8')).hexdigest()


def parse_iso_datetime_to_utc(s: str):
    """
    Terima string tanggal ISO dan variasinya.
    Return datetime timezone-aware di UTC, atau None jika gagal.
    """
    if not s or not isinstance(s, str):
        return None
    s_norm = s.strip()
    if " " in s_norm and "T" not in s_norm:
        s_norm = s_norm.replace(" ", "T")
    if s_norm.endswith("Z"):
        s_norm = s_norm[:-1] + "+00:00"
    if len(s_norm) == 10 and s_norm[4] == "-" and s_norm[7] == "-":
        s_norm += "T00:00:00+00:00"
    try:
        dt = datetime.fromisoformat(s_norm)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt


def is_valid_number(nomor_hp: str) -> bool:
    """
    Valid jika:
    - Hanya digit setelah normalisasi +/spasi/dash dihapus
    - Prefix 08... atau 628...
    - Panjang total 10–15 digit
    """
    if not nomor_hp:
        return False
    nomor = (str(nomor_hp).strip()
             .replace('+', '')
             .replace(' ', '')
             .replace('-', ''))
    return re.fullmatch(r'(?:08|628)\d{8,13}', nomor) is not None


def truthy(val):
    """
    Konversi field 'status' dari respons Fonnte ke boolean sukses yang benar.
    Hindari jebakan bool('false') == True.
    """
    if isinstance(val, bool):
        return val is True
    if isinstance(val, (int, float)):
        return val == 1
    if isinstance(val, str):
        return val.strip().lower() in {"true", "1", "ok", "success"}
    return False


def send_whatsapp_and_log(cursor, pemohon_id, nama, nomor_hp, nama_izin, tahapan, no_permohonan, token, user_id, username):
    """Kirim WA ke pemohon + log ke tabel pesans."""
    message = (
        f"Hai Saudara/Saudari {nama},  dokumen permohonan perizinan {nama_izin} dengan Nomor Permohonan : {no_permohonan} "
        f"saat ini sudah pada tahap {tahapan}.\n\n"
        f"Pesan ini dikirim otomatis oleh Sitaku | sitaku.lotusaja.com\n{username or ''}"
    )

    if not is_valid_number(nomor_hp):
        log(f"[PEMOHON] Skip kirim (nomor tidak valid): raw='{nomor_hp}'")
        cursor.execute(
            "INSERT INTO pesans (pemohon_id, user_id, pesan, status, created_at, updated_at) "
            "VALUES (%s, %s, %s, 'gagal', NOW(), NOW())",
            (pemohon_id, user_id, f"[Nomor tidak valid] {message}")
        )
        return

    status = "gagal"
    try:
        res = requests.post(
            FONNTE_URL,
            json={"target": nomor_hp, "message": message, "countryCode": "62"},
            headers={"Authorization": token},
            timeout=REQUEST_TIMEOUT
        )
        resp_json = {}
        try:
            resp_json = res.json()
        except Exception:
            pass
        ok = truthy(resp_json.get("status"))
        status = "terkirim" if ok else "gagal"
        log(f"[PEMOHON] WA -> {nomor_hp} http={res.status_code} resp={resp_json} → status_db={status}")
    except Exception as e:
        log(f"[PEMOHON] Gagal WA -> {nomor_hp}: {e}")
        status = "gagal"

    cursor.execute(
        "INSERT INTO pesans (pemohon_id, user_id, pesan, status, created_at, updated_at) "
        "VALUES (%s, %s, %s, %s, NOW(), NOW())",
        (pemohon_id, user_id, message, status)
    )
    time.sleep(0.2)


def send_wa_to_matching_pegawai_if_needed(conn, cursor, user, tahapan, no_permohonan, pemohon_id, nama_izin, created_at_wib, username, nama_pemohon):
    """
    Kirim WA ke pegawai:
    - hanya jika kirim_pegawai='belum' dan status='proses'
    - hanya ke pegawai dengan posisi == tahapan (case-insensitive, trim)
    - set 'sudah' HANYA bila minimal satu WA terkirim
    """
    cursor.execute(
        "SELECT kirim_pegawai, status FROM pemohons WHERE id=%s AND user_id=%s",
        (pemohon_id, user['id'])
    )
    row = cursor.fetchone()
    if not row:
        log(f"[PEGAWAI] [SKIP] pemohon_id={pemohon_id} user_id={user['id']} tidak ditemukan")
        return

    kp = str(row['kirim_pegawai'] or '').strip().lower()
    st = str(row['status'] or '').strip().lower()
    if kp != 'belum' or st != 'proses':
        log(f"[PEGAWAI] [SKIP] kirim_pegawai={kp} status={st} (butuh: belum/proses)")
        return

    tahap_key = (tahapan or '').strip().lower()
    sent_count = 0

    for pegawai in user.get('pegawais', []):
        pos = (pegawai.get('posisi') or '').strip().lower()
        if pos != tahap_key:
            continue

        nama_pj = pegawai.get('nama') or '-'
        hp_pj = pegawai.get('no_hp') or ''
        pesan = (
            f"Notifikasi Permohonan *{tahapan}*\n"
            f"Nama: {nama_pemohon}\n"
            f"Perihal: {nama_izin}\n"
            f"Nomor: {no_permohonan}\n"
            f"Tgl. Pengajuan: {created_at_wib or '-'}\n\n"
            f"Silakan login ke website sicantik.go.id untuk {tahapan.lower()}.\n\n"
            f"Pesan ini dikirim secara otomatis oleh Sitaku | sitaku.lotusaja.com\n"
            f"{username or 'Sitaku'}"
        )

        if is_valid_number(hp_pj):
            try:
                res = requests.post(
                    FONNTE_URL,
                    json={"target": hp_pj, "message": pesan, "countryCode": "62"},
                    headers={"Authorization": user['fonnte_token']},
                    timeout=REQUEST_TIMEOUT
                )
                resp_json = {}
                try:
                    resp_json = res.json()
                except Exception:
                    pass
                ok = truthy(resp_json.get("status"))
                if ok:
                    sent_count += 1
                log(f"[PEGAWAI] WA -> {hp_pj} http={res.status_code} resp={resp_json} ok={ok}")
            except Exception as e:
                log(f"[PEGAWAI] [WARN] gagal WA {hp_pj}: {e}")
        else:
            log(f"[PEGAWAI] [SKIP] nomor pegawai tidak valid: raw='{hp_pj}'")

        cursor.execute(
            "INSERT INTO notif_pegawais (user_id, nomor_hp, nama, posisi, pesan, created_at, updated_at) "
            "VALUES (%s, %s, %s, %s, %s, NOW(), NOW())",
            (user['id'], hp_pj, nama_pj, tahapan, pesan)
        )

    if sent_count > 0:
        cursor.execute(
            "UPDATE pemohons SET kirim_pegawai='sudah', updated_at=NOW() WHERE id=%s AND user_id=%s",
            (pemohon_id, user['id'])
        )
        conn.commit()
        log(f"[PEGAWAI] [OK] terkirim ke {sent_count} nomor → flag 'sudah'")
    else:
        log("[PEGAWAI] [INFO] Tidak ada pegawai yang dikirimi (mismatch posisi atau nomor invalid). Flag tetap 'belum'.")


# ========================
# Core proses
# ========================

def process_user(conn, user, cursor):
    api_url = user.get('api_url')
    token = user.get('fonnte_token')
    user_id = int(user.get('id'))
    username = user.get('username')

    if not api_url or not token:
        log(f"[SKIP user_id={user_id}] api_url / token kosong")
        return

    log(f"[user_id={user_id}] GET {api_url}")
    try:
        response = requests.get(api_url, headers={'Authorization': API_TOKEN}, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        log(f"[ERROR] Request ke {api_url} gagal: {e}")
        return

    api_data = response.json().get('data', [])
    if isinstance(api_data, dict):
        data_list = api_data.get('data', [])  # dukung v2
    else:
        data_list = api_data if isinstance(api_data, list) else []

    log(f"[user_id={user_id}] items dari API: {len(data_list)}")

    seen_ids = []
    for item in data_list:
        ext_id = item.get('id')
        log(f"[user_id={user_id}] item ext_id={ext_id} tahap='{item.get('nama_proses')}' no='{item.get('no_permohonan')}'")
        if not ext_id:
            log("[WARN] ext_id kosong, item di-skip.")
            continue

        seen_ids.append(ext_id)

        nama = item.get('nama')
        hp = item.get('no_hp')
        no_perm = item.get('no_permohonan')
        nama_izin = item.get('jenis_izin')
        tahapan = item.get('nama_proses')
        raw_tgl = item.get('tgl_pengajuan')

        dt_utc = parse_iso_datetime_to_utc(raw_tgl)
        tgl_utc = dt_utc.strftime('%Y-%m-%d %H:%M:%S') if dt_utc else None
        tgl_wib = dt_utc.astimezone(WIB).strftime('%Y-%m-%d %H:%M:%S') if dt_utc else None

        payload_hash = compute_hash(item)

        cursor.execute(
            "SELECT * FROM pemohons WHERE external_id=%s AND user_id=%s",
            (ext_id, user_id)
        )
        row = cursor.fetchone()

        if not row:
            # Baris baru → status 'proses'
            log(f"[user_id={user_id}] NEW ext_id={ext_id} tahapan='{tahapan}'")
            cursor.execute(
                """
                INSERT INTO pemohons (
                    external_id, user_id, nama, nomor_hp, no_permohonan, nama_izin,
                    tahapan, status, payload_hash, tgl_pengajuan,
                    last_notified_tahapan, notified_at, kirim_pegawai, created_at
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,'proses',%s,%s,%s,NOW(),'belum',NOW())
                """,
                (ext_id, user_id, nama, hp, no_perm, nama_izin, tahapan, payload_hash, tgl_utc, tahapan)
            )
            new_id = cursor.lastrowid
            send_whatsapp_and_log(cursor, new_id, nama, hp, nama_izin, tahapan, no_perm, token, user_id, username)
            send_wa_to_matching_pegawai_if_needed(conn, cursor, user, tahapan, no_perm, new_id, nama_izin, tgl_wib, username, nama)
        else:
            # Jika sebelumnya selesai dan muncul lagi → hidupkan
            if str(row['status'] or '').lower() == 'selesai':
                log(f"[user_id={user_id}] RE-APPEAR ext_id={ext_id} → set 'proses'")
                cursor.execute(
                    "UPDATE pemohons SET status='proses', nama_izin=%s WHERE id=%s AND user_id=%s",
                    (nama_izin, row['id'], user_id)
                )

            old_tahap = row['tahapan']
            tahapan_changed = (old_tahap != tahapan)
            hash_changed = (row['payload_hash'] != payload_hash)
            number_changed = (row['nomor_hp'] != hp)  # <<< penting: deteksi nomor berubah

            if tahapan_changed:
                # Reset pegawai, panggil pengiriman
                log(f"[user_id={user_id}] Tahap {old_tahap} → {tahapan} (id={row['id']}) → reset kirim_pegawai='belum'")
                cursor.execute(
                    "UPDATE pemohons SET kirim_pegawai='belum' WHERE id=%s AND user_id=%s",
                    (row['id'], user_id)
                )
                conn.commit()
                # Begitu di-reset, langsung coba kirim ke pegawai
                send_wa_to_matching_pegawai_if_needed(conn, cursor, user, tahapan, no_perm, row['id'], nama_izin, tgl_wib, username, nama)

                # Kirim WA ke pemohon jika belum pernah dinotice di tahapan ini
                if row['last_notified_tahapan'] != tahapan:
                    send_whatsapp_and_log(cursor, row['id'], nama, hp, nama_izin, tahapan, no_perm, token, user_id, username)
                    cursor.execute(
                        "UPDATE pemohons SET last_notified_tahapan=%s, notified_at=NOW() WHERE id=%s AND user_id=%s",
                        (tahapan, row['id'], user_id)
                    )

            # Nomor berubah tapi tahapan tidak berubah → kirim WA ke nomor baru + log
            if (not tahapan_changed) and number_changed:
                log(f"[user_id={user_id}] Nomor telepon berubah: {row['nomor_hp']} -> {hp} (ext_id={ext_id})")
                send_whatsapp_and_log(cursor, row['id'], nama, hp, nama_izin, tahapan, no_perm, token, user_id, username)

            # Fallback: data sama tapi flag masih 'belum' & status 'proses' → kirim pegawai sekarang
            if not tahapan_changed and not hash_changed:
                kp = str(row['kirim_pegawai'] or '').strip().lower()
                st = str(row['status'] or '').strip().lower()
                if kp == 'belum' and st == 'proses':
                    log(f"[user_id={user_id}] Fallback kirim pegawai (ext_id={ext_id}) karena flag 'belum' & status 'proses'")
                    send_wa_to_matching_pegawai_if_needed(conn, cursor, user, tahapan, no_perm, row['id'], nama_izin, tgl_wib, username, nama)

            # Sinkron kolom lain bila ada perubahan
            if hash_changed or tahapan_changed or number_changed:
                cursor.execute(
                    """
                    UPDATE pemohons
                    SET nama=%s, nomor_hp=%s, no_permohonan=%s, nama_izin=%s,
                        tahapan=%s, status='proses', payload_hash=%s, tgl_pengajuan=%s
                    WHERE external_id=%s AND user_id=%s
                    """,
                    (nama, hp, no_perm, nama_izin, tahapan, payload_hash, tgl_utc, ext_id, user_id)
                )

    # Tandai yang hilang di API sebagai 'selesai'
    if seen_ids:
        placeholders = ','.join(['%s'] * len(seen_ids))
        sql = f"""
            UPDATE pemohons
            SET status='selesai'
            WHERE user_id=%s AND external_id NOT IN ({placeholders}) AND status!='selesai'
        """
        cursor.execute(sql, [user_id] + seen_ids)
        log(f"[user_id={user_id}] Mark 'selesai' untuk yang tidak ada di API (ke-{len(seen_ids)} ID terlihat)")

    conn.commit()


def main():
    log("=== START SYNC ===")
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)

    log(f"GET users: {USER_API}")
    try:
        response = requests.get(USER_API, headers={'Authorization': API_TOKEN}, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        log(f"[ERROR] Gagal ambil users: {e}")
        return

    users = response.json().get('data', [])
    log(f"Users fetched: {len(users)}")

    for i, user in enumerate(users, 1):
        log(f"-- Process user #{i} id={user.get('id')} username={user.get('username')} api_url={user.get('api_url')}")
        try:
            process_user(conn, user, cursor)
        except Exception as e:
            log(f"[ERROR] Unhandled error process user id={user.get('id')}: {e}")

    cursor.close()
    conn.close()
    log("=== DONE SYNC ===")


if __name__ == '__main__':
    # jalankan tanpa buffer supaya log realtime (opsional: python -u fetch_pemohon2.py)
    main()
