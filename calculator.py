#!/usr/bin/env python3
import database
import asyncio
import multiprocessing
import filters
import warnings
warnings.filterwarnings("ignore")

class Downloader:
    def __init__(self, db):
        self.db = db
        self.complete = False
        
    async def task(self):
        while True:
            materials = self.db.getitems("pending_materials").fetchmany(100)
            materials = [m["material_id"] for m in materials]
            if len(materials) == 0:
                self.complete = True
                await asyncio.sleep(1)
                continue
            self.db.add_materials(materials)
            self.complete = False
            await asyncio.sleep(0)

class Filter:
    def __init__(self, db, table_in, col_in, value_in,
                 table_out, filter_func, pool):
        self.db = db
        self.table_in = table_in
        self.col_in = col_in
        self.value_in = value_in
        self.table_out = table_out
        self.filter_func = filter_func
        self.pool = pool
        self.inprocess = dict()
        self.complete = False
        self.db.create_table(table_out, [("is" + table_out, "INTEGER")])

    async def task(self):
        max_targets = 1000
        while True:
            # Get complete tasks
            complete = []
            for t, v in self.inprocess.items():
                try:
                    r = v.get(0)
                    complete.append((t, r))
                except multiprocessing.TimeoutError:
                    continue
            
            # save complete tasks
            for t, r in complete:
                self.db.setitem(self.table_out, (t,r))
                del self.inprocess[t]

            # Get new tasks                
            targets = self.db.getitems(self.table_in,
                                           self.col_in,
                                           self.value_in,
                                           not_table=self.table_out,
                                           not_col1="material_id",
                                           not_col2="material_id")
            new_tasks = []
            for t in targets:
                if t["material_id"] not in self.inprocess:
                    task =self.db.getitems("materials",
                                           "material_id",
                                           t["material_id"]).fetchall()[0]
                    new_tasks.append(task)
                    if len(new_tasks) >= max_targets - len(self.inprocess):
                        break            

            # Add new tasks to pool
            self.inprocess.update({ t["material_id"]:
                                    self.pool.apply_async(self.filter_func,
                                                          (t,))
                                    for t in new_tasks})
            self.complete = len(new_tasks) == 0
            await asyncio.sleep(1)

async def main():
    db = database.Database()    
    with multiprocessing.Pool(4) as pool:
        tasks = []
        tasks.append(Downloader(db))
        tasks.append(Filter(db, "materials", None, None,
                    "exfoliable", filters.exfoliable2, pool))
        tasks.append(Filter(db, "exfoliable", "isexfoliable", 1,
                    "layered", filters.islayered, pool))

        for t in tasks:
            asyncio.create_task(t.task())

        prev_complete = False
        while True:
            complete = all((t.complete for t in tasks))
            if not prev_complete and complete:
                print("All tasks complete")
            prev_complete = complete
            await asyncio.sleep(1)

asyncio.run(main())
