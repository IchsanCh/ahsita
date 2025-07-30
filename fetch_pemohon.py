import requests
import mysql.connector
import hashlib
import json
import time
from datetime import datetime

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

def compute_hash(item):
    raw = json.dumps(item, sort_keys=True, ensure_ascii=False)
    return hashlib.md5(raw.encode('utf-8')).hexdigest()

def is_valid_number(nomor_hp):
    if not nomor_hp:
        return False
    nomor = nomor_hp.strip().replace('+', '').replace(' ', '').replace('-', '')
    return nomor.isdigit() and (nomor.startswith("08") or nomor.startswith("628")) and 10 <= len(nomor) <= 15

def send_whatsapp_and_log(cursor, pemohon_id, nama, nomor_hp, nama_izin, tahapan, no_permohonan, token, user_id, username):
    message = f"""Hai Saudara/Saudari {nama}, dokumen permohonan perizinan {nama_izin} dengan Nomor Permohonan : {no_permohonan} saat ini sudah pada tahap {tahapan}.

Pesan ini dikirim secara otomatis oleh Sitaku | sitaku.lotusaja.com
{username}"""

    if not is_valid_number(nomor_hp):
        print(f"Nomor tidak valid: {nomor_hp}")
        cursor.execute("""
            INSERT INTO pesans (pemohon_id, user_id, pesan, status, created_at, updated_at)
            VALUES (%s, %s, %s, %s, NOW(), NOW())
        """, (pemohon_id, user_id, f"[Nomor tidak valid] {message}", "gagal"))
        return

    try:
        res = requests.post(FONNTE_URL, json={
            "target": nomor_hp,
            "message": message,
            "countryCode": "62"
        }, headers={"Authorization": token}, timeout=30)

        response_json = res.json()
        print(f"WA ke {nomor_hp}: {res.text}")
        status = "terkirim" if response_json.get("status") else "gagal"
    except Exception as e:
        print(f"Gagal WA ke {nomor_hp}: {e}")
        status = "gagal"

    cursor.execute("""
        INSERT INTO pesans (pemohon_id, user_id, pesan, status, created_at, updated_at)
        VALUES (%s, %s, %s, %s, NOW(), NOW())
    """, (pemohon_id, user_id, message, status))
    time.sleep(0.25)

def send_wa_to_matching_pegawai_if_needed(conn, cursor, user, tahapan, no_permohonan=None, pemohon_id=None, nama_izin=None, created_at=None, username=None):
    if not pemohon_id:
        return

    cursor.execute("SELECT kirim_pegawai, status FROM pemohons WHERE id = %s", (pemohon_id,))
    row = cursor.fetchone()
    if not row or str(row['kirim_pegawai']).lower() != 'belum' or str(row['status']).lower() != 'proses':
        print(f"Pemohon ID {pemohon_id} tidak perlu dikirimi WA pegawai (status/kirim_pegawai)")
        return

    for pegawai in user.get("pegawais", []):
        if pegawai.get("posisi") != tahapan:
            continue

        nama = pegawai.get("nama")
        nomor_hp = pegawai.get("no_hp")
        message = f"""Notifikasi Permohonan {tahapan}
Nama: {nama}
Perihal: {nama_izin}
Nomor: {no_permohonan}
Tgl. Permohonan: {created_at}

Silakan login ke website sicantik.go.id untuk {tahapan.lower()}.

Pesan ini dikirim secara otomatis oleh Sitaku | sitaku.lotusaja.com
{username}"""


        if is_valid_number(nomor_hp):
            try:
                res = requests.post(FONNTE_URL, json={
                    "target": nomor_hp,
                    "message": message,
                    "countryCode": "62"
                }, headers={"Authorization": user['fonnte_token']}, timeout=10)
                print(f"WA Pegawai ke {nomor_hp}: {res.text}")
            except Exception as e:
                print(f"Gagal kirim WA ke pegawai {nomor_hp}: {e}")
        else:
            print(f"Nomor pegawai tidak valid: {nomor_hp}")

        cursor.execute("""
            INSERT INTO notif_pegawais (user_id, nomor_hp, nama, posisi, pesan, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, NOW(), NOW())
        """, (user['id'], nomor_hp, nama, pegawai.get("posisi"), message))
    cursor.execute("UPDATE pemohons SET kirim_pegawai = 'sudah', updated_at = NOW() WHERE id = %s and user_id = %s", (pemohon_id, user['id']))
    conn.commit()
    print(f"Update kirim_pegawai jadi 'sudah' untuk pemohon ID {pemohon_id}")

def process_user(conn, user, cursor):
    api_url = user.get("api_url")
    token = user.get("fonnte_token")
    user_id = int(user.get("id"))
    username = user.get("username")
    
    if not api_url or not token:
        print(f"Skip user ID {user.get('id')} karena data tidak lengkap.")
        return

    try:
        res = requests.get(api_url, timeout=30)
        res.raise_for_status()

        # Ambil respons JSON dan dukung kedua format v1 dan v2
        json_resp = res.json()
        raw_data = json_resp.get("data")
        if isinstance(raw_data, dict) and "data" in raw_data:
            data_list = raw_data["data"]
        elif isinstance(raw_data, list):
            data_list = raw_data
        else:
            data_list = []

        for item in data_list:
            ext_id = item.get("id")
            nama = item.get("nama")
            nomor_hp = item.get("no_hp")
            no_permohonan = item.get("no_permohonan")
            nama_izin = item.get("jenis_izin")
            tahapan = item.get("nama_proses")
            status = item.get("status")
            created_at_raw = item.get("tgl_pengajuan")  # contoh: "2025-07-25T09:00:00+00:00"
            created_at = None

            if created_at_raw:
                try:
                    dt_obj = datetime.fromisoformat(created_at_raw)
                    created_at = dt_obj.strftime("%Y-%m-%d %H:%M:%S")  
                except Exception as e:
                    print(f"Gagal parsing tgl_pengajuan: {e}")
                    created_at = None
            hash_val = compute_hash(item)

            cursor.execute("SELECT id, tahapan, payload_hash, last_notified_tahapan, nomor_hp FROM pemohons WHERE external_id = %s AND user_id = %s", (ext_id, user_id))
            result = cursor.fetchone()

            if result is None:
                print(f"Pemohon baru: ID {ext_id}")
                cursor.execute("""
                    INSERT INTO pemohons
                    (external_id, user_id, nama, nomor_hp, no_permohonan, nama_izin, tahapan, status, payload_hash,
                     tgl_pengajuan, last_notified_tahapan, notified_at, kirim_pegawai, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'belum', NOW())
                """, (ext_id, user_id, nama, nomor_hp, no_permohonan, nama_izin, tahapan, status, hash_val,
                      created_at, tahapan, datetime.now()))
                new_id = cursor.lastrowid
                send_whatsapp_and_log(cursor, new_id, nama, nomor_hp, nama_izin, tahapan, no_permohonan, token, user_id, username)
                send_wa_to_matching_pegawai_if_needed(conn, cursor, user, tahapan, no_permohonan, new_id, nama_izin, created_at, username)

            elif result['payload_hash'] != hash_val:
                old_tahapan = result['tahapan']
                pemohon_id = result['id']
                last_notified = result['last_notified_tahapan']

                if tahapan != last_notified or nomor_hp != result.get('nomor_hp'):
                    cursor.execute("UPDATE pemohons SET kirim_pegawai = 'belum' WHERE id = %s and user_id=%s", (pemohon_id, user_id))
                    conn.commit()
                    print(f"Tahapan berubah: ID {pemohon_id} {old_tahapan} → {tahapan} {pemohon_id}")
                    send_whatsapp_and_log(cursor, pemohon_id, nama, nomor_hp, nama_izin, tahapan, no_permohonan, token, user_id, username)
                    send_wa_to_matching_pegawai_if_needed(conn, cursor, user, tahapan, no_permohonan, pemohon_id, nama_izin, created_at, username)

                    cursor.execute("""
                        UPDATE pemohons
                        SET nama=%s, nomor_hp=%s, no_permohonan=%s, nama_izin=%s,
                            tahapan=%s, status=%s, payload_hash=%s,
                            tgl_pengajuan=%s, last_notified_tahapan=%s,
                            notified_at=%s
                        WHERE external_id=%s and user_id=%s
                    """, (nama, nomor_hp, no_permohonan, nama_izin, tahapan, status,
                          hash_val, created_at, tahapan, datetime.now(), ext_id, user_id))
                else:
                    print(f"Data update tanpa WA: ID {pemohon_id}")
                    cursor.execute("""
                        UPDATE pemohons 
                        SET nama=%s, nomor_hp=%s, no_permohonan=%s, nama_izin=%s,
                            tahapan=%s, status=%s, payload_hash=%s, tgl_pengajuan=%s
                        WHERE external_id=%s and user_id=%s
                    """, (nama, nomor_hp, no_permohonan, nama_izin, tahapan, status, hash_val, created_at, ext_id, user_id))

            else:
                pemohon_id = result['id']
                print(f"Skip (data sama): ID {pemohon_id}")
                cursor.execute("SELECT kirim_pegawai, status FROM pemohons WHERE id = %s AND user_id = %s", (pemohon_id, user_id))
                row = cursor.fetchone()
                if row and row['kirim_pegawai'].lower() == 'belum' and row['status'].lower() == 'proses':
                    send_wa_to_matching_pegawai_if_needed(conn, cursor, user, tahapan, no_permohonan, pemohon_id, nama_izin, created_at, username)

    except Exception as e:
        print(f"Error saat proses user ID {user.get('id')}: {e}")

def main():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)
        headers = {"Authorization": API_TOKEN}
        response = requests.get(USER_API, headers=headers, timeout=30)
        response.raise_for_status()
        users = response.json().get("data", [])

        for user in users:
            process_user(conn, user, cursor)

        conn.commit()
        cursor.close()
        conn.close()
        print("Semua sinkronisasi selesai.")
    except Exception as e:
        print("Gagal memulai sinkron:", str(e))

if __name__ == "__main__":
    main()