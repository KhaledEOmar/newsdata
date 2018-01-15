import psycopg2

def main(): 
        db = psycopg2.connect("dbname=news")

        cursor = db.cursor()

        print "Connected!\n"

        cursor.execute("select path as path, count(*) as count FROM log WHERE path LIKE '%article%' GROUP BY path ORDER BY count(*) DESC LIMIT 3;")
        #/article/ then slug
        topthree = cursor.fetchall()
        print topthree

        cursor.execute("select path, count(*) as count FROM log WHERE path LIKE '%article%' GROUP BY path ORDER BY count(*) DESC LIMIT 3;")
        topauthors = cursor.fetchall()
        print topauthors

        cursor.execute("select path as path, count(*) as count FROM log WHERE path LIKE '%article%' GROUP BY path ORDER BY count(*) DESC LIMIT 3;")
        topthree = cursor.fetchall()
        print topthree

        db.close()

main()
