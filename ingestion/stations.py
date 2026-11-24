"""The 24 stations I sample: 2 per Berlin Bezirk, covering S-Bahn, U-Bahn, Tram and Bus.

Think of these as the 24 street corners where a traffic counter stands.
Each station was checked against the official Bezirk boundaries (Berlin Geoportal,
layer "alkis_bezirke"), so every district has exactly two.

The `bezirk` value here is only a label for readability. In the warehouse, the district
is assigned again from the station's coordinates (point-in-polygon), so the data,
not this file, is the source of truth.

The `lines` list is used only to generate the synthetic practice dataset.
Each entry is (line name, product), where product is one of:
suburban (S-Bahn), subway (U-Bahn), tram, bus.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Station:
    """One sampled station."""

    station_id: str  # VBB stop ID, e.g. "900100003"
    name: str  # Display name
    bezirk: str  # Expected Bezirk (label only)
    latitude: float
    longitude: float
    lines: tuple[tuple[str, str], ...]  # (line name, product) for synthetic data


STATIONS: tuple[Station, ...] = (
    # --- Mitte ---
    Station("900100003", "S+U Alexanderplatz", "Mitte", 52.521508, 13.411267,
            (("S5", "suburban"), ("S7", "suburban"), ("U2", "subway"), ("U5", "subway"),
             ("U8", "subway"), ("M4", "tram"), ("M5", "tram"), ("100", "bus"), ("200", "bus"))),
    Station("900003201", "S+U Berlin Hauptbahnhof", "Mitte", 52.525607, 13.369072,
            (("S5", "suburban"), ("S7", "suburban"), ("U5", "subway"), ("M5", "tram"),
             ("M10", "tram"), ("120", "bus"))),
    # --- Friedrichshain-Kreuzberg ---
    Station("900013102", "U Kottbusser Tor", "Friedrichshain-Kreuzberg", 52.499044, 13.417749,
            (("U1", "subway"), ("U3", "subway"), ("U8", "subway"), ("M29", "bus"), ("140", "bus"))),
    Station("900120004", "S+U Warschauer Str.", "Friedrichshain-Kreuzberg", 52.505768, 13.449157,
            (("S5", "suburban"), ("S7", "suburban"), ("S75", "suburban"), ("U1", "subway"),
             ("U3", "subway"), ("M10", "tram"), ("M13", "tram"), ("347", "bus"))),
    # --- Pankow ---
    Station("900130002", "S+U Pankow", "Pankow", 52.567281, 13.412283,
            (("S2", "suburban"), ("S8", "suburban"), ("U2", "subway"), ("M1", "tram"), ("250", "bus"))),
    Station("900110003", "S Greifswalder Str.", "Pankow", 52.540727, 13.438352,
            (("S41", "suburban"), ("S42", "suburban"), ("M4", "tram"), ("M10", "tram"), ("156", "bus"))),
    # --- Charlottenburg-Wilmersdorf ---
    Station("900024102", "S Westkreuz", "Charlottenburg-Wilmersdorf", 52.501148, 13.283036,
            (("S3", "suburban"), ("S5", "suburban"), ("S41", "suburban"), ("S42", "suburban"))),
    Station("900023201", "S+U Zoologischer Garten", "Charlottenburg-Wilmersdorf", 52.506919, 13.332711,
            (("S5", "suburban"), ("S7", "suburban"), ("U2", "subway"), ("U9", "subway"),
             ("100", "bus"), ("200", "bus"), ("M45", "bus"))),
    # --- Neukölln ---
    Station("900078102", "U Rathaus Neukölln", "Neukölln", 52.481147, 13.43481,
            (("U7", "subway"), ("M41", "bus"), ("104", "bus"))),
    Station("900083201", "U Rudow", "Neukölln", 52.415714, 13.49653,
            (("U7", "subway"), ("171", "bus"), ("X11", "bus"))),
    # --- Spandau ---
    Station("900029302", "S+U Rathaus Spandau", "Spandau", 52.535639, 13.199841,
            (("S3", "suburban"), ("S9", "suburban"), ("U7", "subway"), ("M32", "bus"), ("136", "bus"))),
    Station("900034102", "U Haselhorst", "Spandau", 52.538687, 13.231942,
            (("U7", "subway"), ("139", "bus"))),
    # --- Steglitz-Zehlendorf ---
    Station("900062202", "S+U Rathaus Steglitz", "Steglitz-Zehlendorf", 52.455069, 13.322148,
            (("S1", "suburban"), ("U9", "subway"), ("M48", "bus"), ("186", "bus"))),
    Station("900049201", "S Zehlendorf", "Steglitz-Zehlendorf", 52.431212, 13.259224,
            (("S1", "suburban"), ("X10", "bus"), ("101", "bus"))),
    # --- Tempelhof-Schöneberg ---
    Station("900068201", "S+U Tempelhof", "Tempelhof-Schöneberg", 52.470575, 13.3859,
            (("S41", "suburban"), ("S42", "suburban"), ("S45", "suburban"), ("U6", "subway"), ("140", "bus"))),
    Station("900058101", "S Südkreuz", "Tempelhof-Schöneberg", 52.475501, 13.365548,
            (("S2", "suburban"), ("S25", "suburban"), ("S41", "suburban"), ("S42", "suburban"),
             ("M46", "bus"), ("106", "bus"))),
    # --- Treptow-Köpenick ---
    Station("900192001", "S Schöneweide", "Treptow-Köpenick", 52.45461, 13.510149,
            (("S8", "suburban"), ("S9", "suburban"), ("S45", "suburban"), ("S47", "suburban"),
             ("M17", "tram"), ("63", "tram"), ("165", "bus"))),
    Station("900180001", "S Köpenick", "Treptow-Köpenick", 52.458692, 13.579546,
            (("S3", "suburban"), ("164", "bus"), ("269", "bus"))),
    # --- Lichtenberg ---
    Station("900160004", "S+U Lichtenberg", "Lichtenberg", 52.510676, 13.498355,
            (("S5", "suburban"), ("S7", "suburban"), ("S75", "suburban"), ("U5", "subway"),
             ("21", "tram"), ("240", "bus"))),
    Station("900152001", "S Hohenschönhausen", "Lichtenberg", 52.565636, 13.512324,
            (("S75", "suburban"), ("M17", "tram"), ("256", "bus"))),
    # --- Marzahn-Hellersdorf ---
    Station("900170001", "S Marzahn", "Marzahn-Hellersdorf", 52.543271, 13.541521,
            (("S7", "suburban"), ("M6", "tram"), ("M8", "tram"), ("195", "bus"))),
    Station("900175007", "U Hellersdorf", "Marzahn-Hellersdorf", 52.536331, 13.605327,
            (("U5", "subway"), ("M6", "tram"), ("18", "tram"), ("195", "bus"))),
    # --- Reinickendorf ---
    Station("900089301", "U Alt-Tegel", "Reinickendorf", 52.589646, 13.283818,
            (("U6", "subway"), ("133", "bus"), ("124", "bus"))),
    Station("900096101", "S+U Wittenau", "Reinickendorf", 52.596451, 13.335524,
            (("S25", "suburban"), ("U8", "subway"), ("X21", "bus"))),
)
