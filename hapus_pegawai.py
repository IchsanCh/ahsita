import mysql.connector
from datetime import datetime, timedelta

# --- DATABASE CONFIG ---
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'database': 'notifsan'
}

def delete_old_notif_pegawais():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        batas_waktu = (datetime.now() - timedelta(days=3)).strftime('%Y-%m-%d %H:%M:%S')
        query = "DELETE FROM notif_pegawais WHERE created_at < %s"

        cursor.execute(query, (batas_waktu,))
        jumlah_dihapus = cursor.rowcount
        conn.commit()

        print(f"✅ {jumlah_dihapus} data notif_pegawais lebih dari 3 bulan berhasil dihapus.")

        cursor.close()
        conn.close()
    except Exception as e:
        print(f"🚨 Gagal menghapus data lama: {e}")

if __name__ == "__main__":
    delete_old_notif_pegawais()
