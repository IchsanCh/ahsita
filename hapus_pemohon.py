import mysql.connector
from datetime import datetime, timedelta

# --- DATABASE CONFIG ---
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'database': 'notifsan'
}

def delete_old_pesans():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        batas_waktu = datetime.now() - timedelta(days=0)

        cursor.execute("""
            DELETE FROM pesans 
            WHERE created_at < %s
        """, (batas_waktu,))
        jumlah = cursor.rowcount

        conn.commit()
        cursor.close()
        conn.close()

        print(f"✅ {jumlah} data pesan lebih dari 3 bulan berhasil dihapus.")
    except Exception as e:
        print(f"🚨 Gagal menghapus data pesan lama: {e}")

if __name__ == "__main__":
    delete_old_pesans()
