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
    ("ZUERICH", "BADEN", 20, 0),
    ("BADEN", "FRICK", 20, 0),
    ("FRICK", "PRATTELN", 25, 0),
    ("PRATTELN", "BASEL", 5, 0),
    ("BADEN", "AARAU", 15, 0),
    ("AARAU", "FRICK", 20, 0), ("FRICK", "AARAU", 20, 0),
    ("AARAU", "EGERKINGEN", 20, 0),
    ("EGERKINGEN", "PRATTELN", 25, 0)
]

route = ["ZUERICH", "BASEL"]
