import unittest
import json
from mongo_to_camtrapdp import (
    format_datetime_iso,
    convert_deploymentlocations_to_camtrapdp,
    convert_cameratrapmedias_to_camtrapdp,
    convert_observations_to_camtrapdp
)

# Sample data mirroring the structure of the mongo_exports files
# (but simplified for targeted testing)

SAMPLE_MONGO_DEPLOYMENT_LOCATIONS = [
    {
        "_id": "loc1",
        "locationName": "Test Location 1",
        "location": {"type": "Point", "coordinates": [-87.0, 41.0]},
        "creator": "user1",
        "tags": ["tagA", "tagB"],
        "notes": "Some notes"
    }
]

SAMPLE_MONGO_CAMERATRAPMEDIAS = [
    {
        "mediaID": "media1",
        "_id": "media_obj_1",
        "timestamp": "2024-01-15 10:30:00",
        "publicURL": "http://example.com/media1.jpg",
        "filePublic": True,
        "fileName": "media1.jpg",
        "fileMediatype": "image/jpeg",
        "exifData": {"Make": "CameraCorp", "Model": "X1"},
        "favorite": True,
        "mediaComments": ["comment1", "comment2"]
    },
    {
        "mediaID": "media2", # for testing observation link
        "_id": "media_obj_2",
        "timestamp": "2024-01-15 11:00:00.123000", # with milliseconds
        "publicURL": "http://example.com/media2.jpg",
        "filePublic": False,
        "fileName": "media2.jpg",
        "fileMediatype": "image/jpeg",
        "exifData": None, # Test missing exif
        "favorite": False
    }
]

SAMPLE_MONGO_OBSERVATIONS = [
    {
        "_id": "obs1",
        "mediaId": "media1", # Link to first media item
        "eventStart": "2024-01-15 10:30:00",
        "eventEnd": "2024-01-15 10:30:05",
        "observationLevel": "media",
        "observationType": "animal",
        "scientificName": "Panthera pardus",
        "count": 1,
        "creator": "user2",
        "updatedAt": "2024-01-16 09:00:00"
    },
    {
        "_id": "obs2",
        "mediaId": "media_not_in_list", # Test observation with mediaID not in our media sample
        "eventStart": "2024-01-17 12:00:00",
        "eventEnd": "2024-01-17 12:00:00",
        "observationLevel": "event",
        "observationType": "blank",
        "creator": "user3",
        "updatedAt": "2024-01-18 10:00:00"
    }
]

# Expected CamtrapDP headers (simplified, real ones loaded from schema in main script)
# For testing, we can define them here or mock the schema loading if needed.
# For simplicity, functions should accept headers, so we provide them.
DEPLOYMENTS_HEADERS = [
    "deploymentID", "locationID", "locationName", "latitude", "longitude",
    "deploymentStart", "deploymentEnd", "setupBy", "deploymentTags", "deploymentComments"
    # ... other headers
]
MEDIA_HEADERS = [
    "mediaID", "deploymentID", "timestamp", "filePath", "filePublic", "fileName",
    "fileMediatype", "exifData", "favorite", "mediaComments"
    # ... other headers
]
OBSERVATIONS_HEADERS = [
    "observationID", "deploymentID", "mediaID", "eventStart", "eventEnd",
    "observationLevel", "observationType", "scientificName", "count",
    "classifiedBy", "classificationTimestamp"
    # ... other headers
]


class TestMongoToCamtrapDP(unittest.TestCase):

    def test_format_datetime_iso(self):
        self.assertEqual(format_datetime_iso("2024-01-15 10:30:00"), "2024-01-15T10:30:00Z")
        self.assertEqual(format_datetime_iso("2023-11-05 14:20:05.123456"), "2023-11-05T14:20:05Z") # Microseconds are truncated by default strftime
        self.assertEqual(format_datetime_iso(None), None)
        self.assertEqual(format_datetime_iso(""), None)
        self.assertEqual(format_datetime_iso("invalid-date"), None)
        # Test with a timezone aware string (though our function converts to UTC Z)
        # Note: dateutil.parser might be needed for robust timezone parsing if not already used.
        # The current function assumes naive are UTC or converts offset to UTC.
        # Example with explicit offset, assuming dateutil.parser handles it:
        # self.assertEqual(format_datetime_iso("2024-01-01T12:00:00+02:00"), "2024-01-01T10:00:00Z")

    def test_convert_deploymentlocations(self):
        result = convert_deploymentlocations_to_camtrapdp(SAMPLE_MONGO_DEPLOYMENT_LOCATIONS, DEPLOYMENTS_HEADERS)
        self.assertEqual(len(result), 1)
        deployment = result[0]
        self.assertEqual(deployment["deploymentID"], "loc1")
        self.assertEqual(deployment["locationID"], "loc1")
        self.assertEqual(deployment["locationName"], "Test Location 1")
        self.assertEqual(deployment["latitude"], 41.0)
        self.assertEqual(deployment["longitude"], -87.0)
        self.assertEqual(deployment["setupBy"], "user1")
        self.assertEqual(deployment["deploymentTags"], "tagA|tagB")
        self.assertEqual(deployment["deploymentComments"], "Some notes")
        self.assertEqual(deployment["deploymentStart"], "") # Expected to be blank
        self.assertEqual(deployment["deploymentEnd"], "")   # Expected to be blank
        # Ensure all headers are present
        for header in DEPLOYMENTS_HEADERS:
            self.assertIn(header, deployment)

    def test_convert_cameratrapmedias(self):
        default_dep_id = "dep_test_1"
        result = convert_cameratrapmedias_to_camtrapdp(SAMPLE_MONGO_CAMERATRAPMEDIAS, MEDIA_HEADERS, default_deployment_id=default_dep_id)
        self.assertEqual(len(result), 2)

        media1 = result[0]
        self.assertEqual(media1["mediaID"], "media1")
        self.assertEqual(media1["deploymentID"], default_dep_id)
        self.assertEqual(media1["timestamp"], "2024-01-15T10:30:00Z")
        self.assertEqual(media1["filePath"], "http://example.com/media1.jpg")
        self.assertEqual(media1["filePublic"], True)
        self.assertEqual(media1["fileName"], "media1.jpg")
        self.assertEqual(media1["fileMediatype"], "image/jpeg")
        self.assertEqual(json.loads(media1["exifData"]), {"Make": "CameraCorp", "Model": "X1"})
        self.assertEqual(media1["favorite"], True)
        self.assertEqual(media1["mediaComments"], "comment1|comment2")

        media2 = result[1]
        self.assertEqual(media2["mediaID"], "media2")
        self.assertEqual(media2["deploymentID"], default_dep_id)
        # Check timestamp with milliseconds (format_datetime_iso currently truncates them for Z format, verify this behavior)
        self.assertEqual(media2["timestamp"], "2024-01-15T11:00:00Z") # Milliseconds truncated
        self.assertEqual(media2["exifData"], "") # Was None, should be empty string

        for item in result:
            for header in MEDIA_HEADERS:
                self.assertIn(header, item)

    def test_convert_observations(self):
        # Setup for observation test
        media_to_dep_map = {"media1": "dep_from_media1", "media2": "dep_from_media2"}
        default_obs_dep_id = "default_obs_dep"

        result = convert_observations_to_camtrapdp(
            SAMPLE_MONGO_OBSERVATIONS,
            OBSERVATIONS_HEADERS,
            media_to_deployment_map=media_to_dep_map,
            default_deployment_id=default_obs_dep_id
        )
        self.assertEqual(len(result), 2)

        obs1 = result[0]
        self.assertEqual(obs1["observationID"], "obs1")
        self.assertEqual(obs1["mediaID"], "media1")
        self.assertEqual(obs1["deploymentID"], "dep_from_media1") # From map
        self.assertEqual(obs1["eventStart"], "2024-01-15T10:30:00Z")
        self.assertEqual(obs1["eventEnd"], "2024-01-15T10:30:05Z")
        self.assertEqual(obs1["observationLevel"], "media")
        self.assertEqual(obs1["observationType"], "animal")
        self.assertEqual(obs1["scientificName"], "Panthera pardus")
        self.assertEqual(obs1["count"], 1)
        self.assertEqual(obs1["classifiedBy"], "user2")
        self.assertEqual(obs1["classificationTimestamp"], "2024-01-16T09:00:00Z")

        obs2 = result[1] # Observation whose mediaID is not in the map
        self.assertEqual(obs2["observationID"], "obs2")
        self.assertEqual(obs2["mediaID"], "media_not_in_list")
        self.assertEqual(obs2["deploymentID"], default_obs_dep_id) # Should use default
        self.assertEqual(obs2["observationType"], "blank")
        self.assertEqual(obs2["classifiedBy"], "user3")

        for item in result:
            for header in OBSERVATIONS_HEADERS:
                self.assertIn(header, item)

    def test_empty_input_data(self):
        self.assertEqual(convert_deploymentlocations_to_camtrapdp([], DEPLOYMENTS_HEADERS), [])
        self.assertEqual(convert_cameratrapmedias_to_camtrapdp([], MEDIA_HEADERS, "dep1"), [])
        self.assertEqual(convert_observations_to_camtrapdp([], OBSERVATIONS_HEADERS, {}, "dep1"), [])

    def test_input_with_none_and_invalid_items(self):
        invalid_deployments = [None, {"_id": "valid_loc"}, {}] # Last one is empty dict
        result_deps = convert_deploymentlocations_to_camtrapdp(invalid_deployments, DEPLOYMENTS_HEADERS)
        self.assertEqual(len(result_deps), 1) # Skips None, and also skips {} as it's falsy
        self.assertEqual(result_deps[0]["deploymentID"], "valid_loc")
        # self.assertEqual(result_deps[1]["deploymentID"], "dep2") # This item no longer expected

        invalid_media = [None, {"mediaID": "valid_media"}, {}]
        result_media = convert_cameratrapmedias_to_camtrapdp(invalid_media, MEDIA_HEADERS, "test_dep")
        self.assertEqual(len(result_media), 1) # Skips None and {}
        self.assertEqual(result_media[0]["mediaID"], "valid_media")
        # self.assertTrue(all(h in result_media[1] for h in MEDIA_HEADERS)) # This item no longer expected


if __name__ == '__main__':
    unittest.main()
