import json

def test_mock_extraction_csv():
    # Mock document representing the cameratrapmedias record with multiple speciesConsensus entries
    mock_doc = {
        "_id": "671b09b9c97e2c0a895a1524",
        "mediaID": "7c50572a982a69f12ca47b808ecbe729",
        "timestamp": "2024-01-27T11:40:21.000Z",
        "publicURL": "https://urbanriverrangers.s3.amazonaws.com/images/2024/animal.JPG",
        "consensusStatus": "Verified",
        "speciesConsensus": [
            {
                "observationType": "animal",
                "scientificName": "Procyon lotor",
                "taxonID": "12345",
                "count": 1,
                "accepted": True,
                "observationCount": 2,
                "_id": "67817a0947dc475bcbf5cc51"
            },
            {
                "observationType": "bird",
                "scientificName": None, # Should be ignored in the export but listed in doc
                "observationCount": 1,
            },
            {
                "observationType": "mammal",
                "scientificName": "", # Coalesce should pick mammal
                "observationCount": 3,
            }
        ],
        "aiResults": [
            {
                "modelName": "speciesnet/PyTorch/v4.0.1a",
                "runDate": "2025-07-07",
                "confHuman": 0.1
            }
        ],
        "videoUrl": "https://urbanriverrangers.s3.amazonaws.com/images/2024/animal.MP4"
    }

    mock_collection = [mock_doc]

    # Simulation of CSV row generation
    all_rows = []
    for doc in mock_collection:
        ai_results = doc.get("aiResults", [])
        conf_human = ""
        if isinstance(ai_results, list) and ai_results:
            conf_human = ai_results[-1].get("confHuman", "")

        species_list = doc.get("speciesConsensus", [])
        for species in species_list:
            sci_name = species.get("scientificName")
            if sci_name is not None:
                # Coalesce scientificName and observationType
                species_id = sci_name or species.get("observationType", "")

                row = {
                    "timestamp": doc.get("timestamp"),
                    "speciesIdentification": species_id,
                    "consensusStatus": doc.get("consensusStatus"),
                    "publicURL": doc.get("publicURL"),
                    "videoUrl": doc.get("videoUrl"),
                    "confHuman": conf_human,
                    "observationCount": species.get("observationCount", "")
                }
                all_rows.append(row)

    print(f"Generated {len(all_rows)} rows from mock data.")

    # We have 3 speciesConsensus items, one has scientificName = None, it should be filtered out by `is not None`
    # Wait, in the code: `if sci_name is not None:`
    # Procyon lotor: not None -> row
    # None: is None -> skip
    # "": not None -> row (coalesce to "mammal")

    assert len(all_rows) == 2
    assert all_rows[0]["speciesIdentification"] == "Procyon lotor"
    assert all_rows[0]["observationCount"] == 2
    assert all_rows[1]["speciesIdentification"] == "mammal"
    assert all_rows[1]["observationCount"] == 3
    assert all_rows[0]["confHuman"] == 0.1
    print("CSV logic test PASSED.")

if __name__ == "__main__":
    test_mock_extraction_csv()
