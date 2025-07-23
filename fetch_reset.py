import mysql.connector

# --- DATABASE CONFIG ---
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'database': 'notifsan'
}

def reset_kirim_pegawai():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        query = """
            UPDATE pemohons
            SET kirim_pegawai = 'belum'
            WHERE status = 'proses'
        """
        cursor.execute(query)
        conn.commit()

        print(f"✅ {cursor.rowcount} baris berhasil diupdate jadi 'belum' pada status 'proses'.")

        cursor.close()
        conn.close()
    except Exception as e:
        print(f"🚨 Gagal update: {e}")

if __name__ == "__main__":
    reset_kirim_pegawai()
