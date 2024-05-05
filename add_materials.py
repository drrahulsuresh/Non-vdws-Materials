#!/usr/bin/env python3
import database

material_props = {
        "pretty_formula": "text",
        "spacegroup__number": "integer",
        "cif": "text",
        "e_above_hull": "real",
        "formation_energy_per_atom": "real",
        "band_gap": "real",
    }

criteria = {
        "material_id":{"$in":["mp-1522","mp-555322","mp-2099","mp-556911","mp-22862","mp-23193","mp-23251","mp-22898","mp-157","mp-48","mp-2418","mp-1634","mp-1023934"]},
        "nelements": {"$lt": 4},
        "spacegroup.symbol": {"$ne": None},
        "e_above_hull": {"$lt": 0.1},
}

db = database.Database()
db.set_material_props(material_props)
db.add_pending_materials(criteria)
