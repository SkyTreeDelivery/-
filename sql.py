import psycopg2

# conn = psycopg2.connect(database="postgres", user="postgres", password="zhang002508", host="localhost", port="5432")
# print("Opened database successfully")

# cur = conn.cursor()
# cur.execute('''INSERT INTO public.road_fragment_topo(
# 	tid, pid1, pid2, rid1, rid2)
# 	VALUES (%s, %s, %s, %s, %s);''',(12,13,14,15,16))

# conn.commit()
# conn.close()

def insertPano_noCurrent(pid,roadId,x,y,ptype,isNode,order):
    conn = psycopg2.connect(database="postgres", user="postgres", password="zhang002508", host="localhost", port="5432")
    print("Opened database successfully")

    cur = conn.cursor()
    cur.execute('''INSERT INTO public.pano_current(
	pid, rid, geom, type, isnode, "order")
	VALUES (%s,%s, ST_GeomFromText('POINT(%s %s)',4326), %s, %s, %s);''',(pid,roadId,x,y,ptype,isNode,order))

    conn.commit()
    conn.close()

insertPano_noCurrent('1','2',1.1,2.2,'street',False,2)