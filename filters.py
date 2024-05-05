from pymatgen.core import Structure
from pymatgen.symmetry.analyzer import SpacegroupAnalyzer
from pymatgen.analysis.structure_analyzer import oxide_type
from pymatgen.analysis.bond_valence import BVAnalyzer
from pymatgen.analysis.diffraction.core import DiffractionPattern
import json

with open("atomic_radius.json", "r") as f:
    atomic_radius = json.load(f)

def derepr(text):
    repl = [('\\"', '"'),
            ("\\'", "'"),
            ("\\n", "\n"),
            ("\\r", "\r"),
            ("\\t", "\t"),
            ("\\\\", "\\"),]
    for r in repl:
        text = text.replace(*r)
    return text

def is_bonded(site1, site2, tol=0.1):
    r1 = atomic_radius.get(site1.specie.symbol, 0)
    r2 = atomic_radius.get(site2.specie.symbol, 0)
    bonded_distance = r1 + r2 + tol
    return site1.distance(site2) < bonded_distance

def count_clusters(structure, tol=0.1):
    sites = structure.sites
    n = len(sites)
    visited = [False] * n
    count = 0
    for i in range(n):
        if not visited[i]:
            stack = [i]
            visited[i] = True
            while stack:
                index = stack.pop()
                for j in range(n):
                    if not visited[j] and is_bonded(sites[index], sites[j], tol):
                        stack.append(j)
                        visited[j] = True
            count += 1
    return count

def islayered(result):
    cif = derepr(result["cif"])

    # Create a Structure object from the CIF data
    structure = Structure.from_str(cif, fmt="cif")
    # Check if the structure is layered
    is_layered = False
    for tol in [0.0, 0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4]:
        unit_cell_clusters = count_clusters(structure, tol=tol)
        supercell_clusters = count_clusters(structure * (3, 3, 3), tol=tol)
        if supercell_clusters == 3 * unit_cell_clusters:
            is_layered = True
            break
    return is_layered


def exfoliable(material):
    try:
        cif = derepr(material["cif"])
        # Get crystal structure data for the material
        structure = Structure.from_str(cif, fmt="cif")

        # Get the space group for the material
        spacegroup = SpacegroupAnalyzer(structure).get_space_group_symbol()
        # Get the oxidation state for the material
        oxidation_state = oxide_type(structure)
        # Check if the oxidation state is correct for the material
        if oxidation_state == "mixed":
            print("mixed")
            return None
        # Calculate the interlayer distance and Van der Waals radii for the material
        bva = BVAnalyzer()
        structure = bva.get_oxi_state_decorated_structure(structure)
        interlayer_distance = structure.lattice.c / 2
        van_der_waals_radii = bva.get_vdw_radii(structure)
        # Determine if the material is exfoliable or not
        if interlayer_distance > 5 * van_der_waals_radii:
            return True
        else:
            return False

    except Exception as e:
        print(e)
        return None


def exfoliable2(material):
    try:
        cif = derepr(material["cif"])
        # Get crystal structure data for the material
        structure = Structure.from_str(cif, fmt="cif")
        
        spacegroup = SpacegroupAnalyzer(structure).get_space_group_symbol()
        # Calculate the diffraction pattern for the material
        diffraction = DiffractionPattern.from_structures([structure])
        # Get the indices of the cleavage planes for the material
        indices = mg.analysis.diffraction.get_unique_families(diffraction).indices()
        # Determine exfoliability based on stacking sequence
        stacking_seq = structure.get_space_group_info()[1].split(":")[-1].strip()
        if stacking_seq.startswith("AB"):
            is_exfoliable = True
        else:
            is_exfoliable = False
        # Determine exfoliability based on electronic structure
        bg = structure.get_band_gap()
        if bg is not None and bg > 1:
            is_exfoliable = True

        if is_exfoliable:
            return True
        else:
            return False

    except Exception as e:
        print(e)
        return None
