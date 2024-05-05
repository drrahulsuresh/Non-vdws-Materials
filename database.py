import sqlite3
from pymatgen.ext.matproj import MPRester

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

class Database:
    api_key = "7vOligyH0Mch9TbV"

    def __init__(self):
        self.con = sqlite3.connect("material.db")
        self.con.row_factory = dict_factory

        self.con.execute('''CREATE TABLE IF NOT EXISTS materials (
                         material_id text PRIMARY KEY);''')

        self.con.execute('''CREATE TABLE IF NOT EXISTS pending_materials (
                         material_id text PRIMARY KEY);''')
        
        self.con.commit()

    def set_material_props(self, material_props):
        material_props = {name.replace(".", "__"): material_props[name] for name in material_props}  
        cur_names = self.con.execute("SELECT name FROM pragma_table_info('materials');").fetchall()
        cur_names = {x["name"] for x in cur_names}
        new_names = set(material_props) | {"material_id"}

        for name in cur_names - new_names:
            self.con.execute(f"ALTER TABLE material DROP COLUMN {name};")

        for name in new_names - cur_names:
            self.con.execute(f"ALTER TABLE materials ADD {name!r} {material_props[name]!r};")
        
        if new_names <= cur_names:
            self.con.commit()
            return
        
        self.con.execute('''REPLACE INTO pending_materials
                            SELECT material_id
                            FROM materials;
                         ''')
        self.con.commit()

    def add_pending_materials(self, criteria):
        with MPRester(self.api_key) as mpr:
            while True:
                try:
                    results = mpr.query(criteria, ["material_id"])
                except Exception as e:
                    print(f'Error: {str(e)}. Retrying in 60 seconds.')
                    time.sleep(60)
                else:
                    break

        for r in results:
            self.con.execute(f'''REPLACE INTO pending_materials 
                                 SELECT {r['material_id']!r} WHERE
                                 NOT EXISTS(SELECT * FROM materials WHERE material_id={r['material_id']!r})
                             ''')

        self.con.commit()
            
    def add_materials(self, materials_id):
        props = self.con.execute("SELECT name FROM pragma_table_info('materials');").fetchall()
        props = [x["name"].replace("__", ".") for x in props]
        with MPRester(self.api_key) as mpr:
            try:
                results = mpr.query({"material_id":{"$in":materials_id}}, list(props))            
            except Exception as e:
                print(f'Error: {str(e)}. Retrying later.')
                return

        for r in results:
            data = ",".join(map(repr, [r.get(name, None) for name in props]))
            self.con.executescript(f'''BEGIN;
                                       REPLACE INTO materials VALUES ({data});
                                       DELETE FROM pending_materials WHERE material_id={r['material_id']!r};
                                       COMMIT;'''
                                   )
        self.con.commit()
            
    def getitems(self, table, key=None, value=None,
                 not_table=None, not_col1=None, not_col2=None):
        sql = f"SELECT * FROM {table!r}"
        if key is not None or not_table is not None:
            sql += " WHERE"
        if key is not None:
            sql += f" {key} = {value!r}"
            if not_table is not None:
                sql += " AND"
        if not_table is not None:
            sql += f" {not_col1} NOT IN (SELECT {not_col2} FROM {not_table})"
        sql += ";"
        return self.con.execute(sql)

    def setitem(self, table, values):
        sql = f"INSERT INTO {table} VALUES (" + ",".join(["?"]*len(values)) + ");"
        self.con.execute(sql, values)
        self.con.commit()

    def create_table(self, name, columns):
        sql = f'''CREATE TABLE IF NOT EXISTS {name} (
                         material_id text PRIMARY KEY,
                         '''
        for n, t in columns:
            sql += f"{n} {t},\n"
        sql += "FOREIGN KEY(material_id) REFERENCES materials(material_id));"
        self.con.execute(sql)        
        self.con.commit()
