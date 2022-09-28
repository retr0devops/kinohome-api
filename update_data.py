from executor import get_session, load_kp_data
from datetime import datetime

conn, cur = get_session()

cur.execute("""SELECT id FROM films ORDER BY id DESC;""")
data = cur.fetchall()
conn.close()
for film in data:
    try:
        print(film['id'], end=' ')
        start_time = datetime.now().timestamp()
        load_kp_data(film['id'])
        print(round((datetime.now().timestamp() - start_time) * 1000), 'ms')
    except Exception as e:
        print(e)
