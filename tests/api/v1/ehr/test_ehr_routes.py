import pytest
from httpx import AsyncClient
from fastapi import status
import uuid
import pytest_asyncio


@pytest.fixture
def ehr_subject_payload() -> dict:
    """
    Provides a valid EHR_STATUS payload with a subject
    """
    return {
        "_type": "EHR_STATUS",
        "subject": {
            "_type": "PARTY_SELF",
            'external_ref': {
                'id': {
                    'value': f"test-subject-{uuid.uuid4()}"
                },
                "namespace": "test.namespace",
                "type": "PERSON"
            }
        }
    }


@pytest_asyncio.fixture
async def created_ehr(client: AsyncClient) -> dict:
    """
    A fixture that creates a new EHR and returns its creation response JSON.
    This avoids repeating the creation logic in every test
    """
    response = await client.post("/ehr", headers={"Prefer": "return=representation"})
    assert response.status_code == status.HTTP_201_CREATED
    return response.json()


# When using pytest the fixtures need to be added as arguments to the test functions, Pytest injects them
@pytest.mark.asyncio
async def test_get_ehr_list_empty(client: AsyncClient):
    """
    Test GET /ehr: Ensure an empty list is returned when no EHRs exist
    """
    response = await client.get("/ehr")
    assert response.status_code == status.HTTP_200_OK
    assert response.json() == []


@pytest.mark.asyncio
async def test_get_ehr_list_success(client: AsyncClient, created_ehr: dict):
    """Test GET /ehr: Retrieves a list of EHRs."""
    # The `created_ehr` fixture already creates one EHR for the test.
    # It's required to create one more to ensure it's possible to retrieve a list
    await client.post("/ehr")

    # Run the request
    response = await client.get("/ehr")

    # Assert the response
    assert response.status_code == status.HTTP_200_OK
    data = response.json()

    # Make sure that the result is a list
    assert isinstance(data, list)
    assert len(data) == 2
    assert "_id" in data[0]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload, expected_subject_namespace",
    [
        # Test creating without a body
        (None, "patients"),

        # Test with a body
        ({"_type": "EHR_STATUS", "subject": {"_type": "PARTY_SELF", "external_ref": {"id": {"value": "sub-123"}, "namespace": "ns.test", "type": "PERSON"}}}, "ns.test"),
    ]
)
async def test_create_ehr_success(client: AsyncClient, payload: dict | None, expected_subject_namespace: str):
    """
    Test POST /ehr: Successfully creates an EHR and with and without a body
    """
    response = await client.post("/ehr", json=payload, headers={"Prefer": "return=representation"})

    # Assert
    assert response.status_code == status.HTTP_201_CREATED
    assert "Location" in response.headers
    assert "ETag" in response.headers

    data = response.json()
    ehr_id = data["ehr_id"]["value"]
    assert ehr_id in response.headers["Location"]
    assert ehr_id in response.headers["ETag"]

    # Verify by retrieving the created EHR
    get_response = await client.get(f"/ehr/{ehr_id}")
    assert get_response.status_code == status.HTTP_200_OK
    retrieved_data = get_response.json()
    assert retrieved_data["ehr_status"]["subject"]["external_ref"]["namespace"] == expected_subject_namespace


@pytest.mark.asyncio
async def test_create_ehr_conflict(client: AsyncClient, ehr_subject_payload: dict):
    """
    Test POST /ehr: Ensure a 409 Conflict is returned if an EHR for a sbuject already exists
    """
    # Create the first EHR
    response1 = await client.post("/ehr", json=ehr_subject_payload)
    assert response1.status_code == status.HTTP_201_CREATED

    # Attempt to create the same EHR resource
    response2 = await client.post("/ehr", json = ehr_subject_payload)
    assert response2.status_code == status.HTTP_409_CONFLICT
    assert "already exists" in response2.json()["detail"]


@pytest.mark.asyncio
async def test_get_ehr_by_id_success(client: AsyncClient, created_ehr: dict):
    """
    Test GET /ehr/{ehr_id}: Successfully retrieve an existing EHR
    """
    ehr_id = created_ehr["ehr_id"]["value"]

    # Retrieve the ehr_id from the EHR GET API
    get_response = await client.get(f"/ehr/{ehr_id}")

    assert get_response.status_code == status.HTTP_200_OK
    data = get_response.json()
    assert data["_id"]["value"] == ehr_id
    assert "ehr_status" in data


@pytest.mark.asyncio
async def test_get_ehr_by_id_not_found(client: AsyncClient):
    """
    Test GET /ehr/{ehr_id}: Ensure 404 is returned for a non-existen EHR
    """

    non_existent_ehr_id = str(uuid.uuid4())

    response = await client.get(f"/ehr/{non_existent_ehr_id}")
    assert response.status_code == status.HTTP_404_NOT_FOUND


@pytest.mark.asyncio
async def test_create_ehr_with_id_success(client: AsyncClient):
    """
    Test PUT /ehr/{ehr_id}: Successfully create an EHR with a specified ID
    """

    client_specified_ehr_id = str(uuid.uuid4())

    response = await client.put(f"/ehr/{client_specified_ehr_id}")

    assert response.status_code == status.HTTP_201_CREATED
    assert response.text == ""
    assert response.headers["Location"] == f"/ehr/{client_specified_ehr_id}"
    assert response.headers["ETag"] == f'"{client_specified_ehr_id}"'

    # Verify by retrieving it
    get_response = await client.get(f"/ehr/{client_specified_ehr_id}")
    assert get_response.status_code == status.HTTP_200_OK


@pytest.mark.asyncio
async def test_create_ehr_with_id_conflict(client: AsyncClient, created_ehr: dict):
    """
    Test PUT /ehr/{ehr_id}: Fail with 409 if EHR ID already exists
    """

    # Arrange: The of the already existing EHR from the fixture
    existing_ehr_id = created_ehr["ehr_id"]["value"]

    # Act: Try to create an EHR with the same ID
    response = await client.put(f"/ehr/{existing_ehr_id}")

    # Assert
    assert response.status_code == status.HTTP_409_CONFLICT
    assert "already exists" in response.json()["detail"]


@pytest.mark.asyncio
async def test_get_ehr_by_subject_success(client: AsyncClient, ehr_subject_payload: dict):
    """
    Test GET /ehr?subject_id=...&subject_namespace=...: Successfully retrieve EHR by subject.
    """

    # Create an EHR with a known subject
    response_create_ehr = await client.post("/ehr", json=ehr_subject_payload, headers={"Prefer": "return=representation"})
    assert response_create_ehr.status_code == status.HTTP_201_CREATED

    subject_id = ehr_subject_payload["subject"]["external_ref"]["id"]["value"]
    subject_namespace = ehr_subject_payload["subject"]["external_ref"]["namespace"]

    # Retrieve the EHR using the query parameters
    response = await client.get(f"/ehr?subject_id={subject_id}&subject_namespace={subject_namespace}")

    # Assert
    assert response.status_code == status.HTTP_200_OK
    data = response.json()

    assert data["_id"]
    assert data["ehr_status"]["subject"]["external_ref"]["id"]["value"] == subject_id
    assert data["ehr_status"]["subject"]["external_ref"]["namespace"] == subject_namespace


@pytest.mark.asyncio
async def test_get_ehr_by_subject_not_found(client: AsyncClient):
    """
    Test GET /ehr?subject_id=...: Fail with 404 for a non-existent subject.
    """
    # Act
    response = await client.get("/ehr?subject_id=non-existent&subject_namespace=non-existent")

    # Assert
    assert response.status_code == status.HTTP_404_NOT_FOUND


@pytest.mark.asyncio
async def test_create_ehr_prefer_minimal(client: AsyncClient):
    """
    Test POST /ehr: Ensure 'Prefer: return=minimal' returns an empty body.
    """
    # Arrange: This is the default behavior, but we can be explicit
    headers = {"Prefer": "return=minimal"}

    # Act
    response = await client.post("/ehr", headers=headers)

    # Assert
    assert response.status_code == status.HTTP_201_CREATED
    assert "Location" in response.headers
    assert "ETag" in response.headers
    # Crucially, the response body should be empty for a minimal return
    assert response.text == ""


@pytest.mark.asyncio
async def test_create_ehr_invalid_payload(client: AsyncClient):
    """
    Test POST /ehr: Ensure 422 is returned for a malformed payload.
    """
    # Arrange: Payload with wrong data type for 'is_modifiable'
    invalid_payload = {
        "subject": {
            "id": "subject-422",
            "namespace": "test.namespace"
        },
        "is_modifiable": "this-should-be-a-boolean"
    }

    # Act
    response = await client.post("/ehr", json=invalid_payload)

    # Assert
    assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
