demo_vertices = [
    ("ZUERICH", "Zürich"),
    ("BASEL", "Basel"),
    ("AARAU", "Aarau"),
    ("BADEN", "Baden"),
    ("FRICK", "Frick"),
    ("EGERKINGEN", "Egerkingen"),
    ("PRATTELN", "Pratteln")
]

demo_edges = [
    ("ZUERICH", "BADEN", 20, 0), ("BADEN", "ZUERICH", 20, 0),
    ("BADEN", "FRICK", 20, 0), ("FRICK", "BADEN", 20, 0),
    ("FRICK", "PRATTELN", 25, 0), ("PRATTELN", "FRICK",  25, 0),
    ("PRATTELN", "BASEL", 5, 0), ("BASEL", "PRATTELN",  5, 0),
    ("ZUERICH", "AARAU", 35, 0), ("AARAU", "ZUERICH",  35, 0),
    ("AARAU", "FRICK", 20, 0), ("FRICK", "AARAU", 20, 0),
    ("AARAU", "EGERKINGEN", 20, 0), ("EGERKINGEN", "AARAU",  20, 0),
    ("EGERKINGEN", "PRATTELN", 25, 0), ("PRATTELN", "EGERKINGEN",  25, 0)
]

route = ["ZUERICH", "BASEL"]
