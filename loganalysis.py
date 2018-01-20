import psycopg2


def main():
        db = psycopg2.connect("dbname=news")

        cursor = db.cursor()

        print "Connected!\n\n"

        cursor.execute("""SELECT Articles.Title, x.count from
                        (SELECT substring(path from 10) as path, count(*)
                        as count FROM log WHERE path LIKE '%article%' AND
                        status = '200 OK' GROUP BY path ORDER BY count(*)
                        DESC LIMIT 3) as x
                        LEFT JOIN Articles ON x.path = Articles.Slug
                        ORDER BY count DESC;""")
        topThree = cursor.fetchall()
        print "\n\n                    Top 3 Articles                   "
        print "-----------------------------------------------------"
        for article in topThree:
                print article[0], " - ", article[1], " views"

        cursor.execute("""Select z.name as name, SUM(z.viewCount) from
                        (SELECT Authors.Name, y.viewCount from
                        (SELECT x.shortPath, Articles.Author, Articles.Title
                        as articleTitle, x.viewCount from
                        (SELECT substring(path from 10) as shortPath, count(*)
                        as viewCount FROM log WHERE path LIKE '%article%' AND
                        status = '200 OK' GROUP BY shortPath) as x
                        LEFT JOIN Articles ON x.shortPath = Articles.Slug) as y
                        INNER JOIN Authors ON y.author = Authors.id) as z
                        GROUP BY name ORDER BY sum DESC;""")
        topAuthors = cursor.fetchall()
        print "\n\n                      Top  Authors                   "
        print "-----------------------------------------------------"
        for author in topAuthors:
                print author[0], " - ", author[1], " views"

        cursor.execute("""Select timez, Percent from (SELECT timez, (100.00 *
        Errors/Total) as Percent from (SELECT date_trunc('day',time) as timez,
        count(status) filter (WHERE status LIKE '4%' OR status LIKE '5%') as
        Errors, count(status) as Total from log GROUP BY timez ORDER BY timez)
        as Totals) as Percentage WHERE Percent >= 1.00;""")
        highErrors = cursor.fetchall()
        print "\n\n                   High Error Days                   "
        print "-----------------------------------------------------"
        for errors in highErrors:
                print errors[0].strftime('%b %d, %Y'), " - ",\
                      "{0:.2f}".format(errors[1]), "% errors"

        db.close()
main()
