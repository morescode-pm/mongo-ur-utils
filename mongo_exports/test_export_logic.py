import json

def test_mock_extraction():
    # Mock document representing the cameratrapmedias record
    mock_doc = {
        "_id": "671b09b9c97e2c0a895a1523",
        "mediaID": "6c50572a982a69f12ca47b808ecbe728",
        "timestamp": "2024-01-27T11:38:21.000Z",
        "publicURL": "https://urbanriverrangers.s3.amazonaws.com/images/2024/2024-01-30_prologis_02/DCIM/100MEDIA/SYFW0052.JPG",
        "consensusStatus": "Pending",
        "speciesConsensus": [
            {
                "observationType": "blank",
                "scientificName": None,
                "taxonID": None,
                "count": 1,
                "accepted": False,
                "observationCount": 2,
                "_id": "67817a0947dc475bcbf5cc50"
            }
        ],
        "aiResults": [
            {
                "modelName": "speciesnet/PyTorch/v4.0.1a",
                "runDate": "2025-07-07",
                "confBlank": 1,
                "confHuman": 0,
                "confAnimal": 0,
                "animalDetections": []
            }
        ],
        "videoUrl": "https://urbanriverrangers.s3.amazonaws.com/images/2024/2024-01-30_prologis_02/DCIM/100MEDIA/SYFW0053.MP4"
    }

    # Mock document where scientificName IS NOT NULL
    mock_doc_animal = {
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
            }
        ],
        "aiResults": [
            {
                "modelName": "speciesnet/PyTorch/v4.0.1a",
                "runDate": "2025-07-07",
                "confBlank": 0,
                "confHuman": 0.1,
                "confAnimal": 0.9,
                "animalDetections": [{"bbox": [0.1, 0.1, 0.2, 0.2], "conf": 0.9}]
            }
        ],
        "videoUrl": "https://urbanriverrangers.s3.amazonaws.com/images/2024/animal.MP4"
    }

    mock_collection = [mock_doc, mock_doc_animal]

    # Simulation of filtering logic: "speciesConsensus.scientificName": {"$ne": None}
    filtered_results = []
    for doc in mock_collection:
        # Pymongo dot notation filter for nested arrays: "speciesConsensus.scientificName": {"$ne": None}
        # returns the document if ANY element in speciesConsensus has scientificName != None
        match = False
        for sc in doc.get("speciesConsensus", []):
            if sc.get("scientificName") is not None:
                match = True
                break

        if match:
            # Simulation of projection logic
            projected_doc = {
                "timestamp": doc.get("timestamp"),
                "speciesConsensus": [{"scientificName": sc.get("scientificName")} for sc in doc.get("speciesConsensus", [])],
                "consensusStatus": doc.get("consensusStatus"),
                "publicURL": doc.get("publicURL"),
                "videoUrl": doc.get("videoUrl"),
                "aiResults": [{"confHuman": ai.get("confHuman")} for ai in doc.get("aiResults", [])]
            }
            filtered_results.append(projected_doc)

    print(f"Filtered {len(filtered_results)} documents from mock data.")
    assert len(filtered_results) == 1
    assert filtered_results[0]["speciesConsensus"][0]["scientificName"] == "Procyon lotor"
    assert filtered_results[0]["aiResults"][0]["confHuman"] == 0.1
    print("Logic test PASSED.")

if __name__ == "__main__":
    test_mock_extraction()
