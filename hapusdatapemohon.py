import mysql.connector
from datetime import datetime, timedelta

# --- DATABASE CONFIG ---
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'database': 'notifsan'
}

def delete_old_pemohon():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        batas_waktu = datetime.now() - timedelta(days=4)

        cursor.execute("""
            DELETE FROM pemohons 
            WHERE updated_at < %s
        """, (batas_waktu,))
        jumlah = cursor.rowcount

        conn.commit()
        cursor.close()
        conn.close()

        print(f"✅ {jumlah} data pemohon lebih dari 4 hari berhasil dihapus.")
    except Exception as e:
        print(f"🚨 Gagal menghapus data pemohon lama: {e}")

if __name__ == "__main__":
    delete_old_pemohon()
