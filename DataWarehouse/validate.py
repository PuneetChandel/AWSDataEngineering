import configparser
import psycopg2

def run_queries(cur):
    queries = [
        "SELECT level, COUNT(*) AS total_plays FROM songplays GROUP BY level;",
        "SELECT user_id, COUNT(*) AS total_plays FROM songplays GROUP BY user_id ORDER BY total_plays DESC LIMIT 10;",
        "SELECT EXTRACT(month FROM start_time) AS month, COUNT(*) AS total_plays FROM songplays GROUP BY month;",
        "SELECT s.title AS song_title, a.name AS artist_name, COUNT(*) AS play_count FROM songplays sp JOIN songs s ON sp.song_id = s.song_id JOIN artists a ON sp.artist_id = a.artist_id GROUP BY s.title, a.name ORDER BY play_count DESC LIMIT 10;",
        "SELECT EXTRACT(weekday FROM start_time) AS weekday, COUNT(*) AS total_plays FROM songplays GROUP BY weekday;",
        "SELECT location, COUNT(*) AS total_plays FROM songplays GROUP BY location;",
    ]

    for query in queries:
        cur.execute(query)
        rows = cur.fetchall()
        for row in rows:
            print(row)


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))

    cur = conn.cursor()

    try:
        run_queries(cur)
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
