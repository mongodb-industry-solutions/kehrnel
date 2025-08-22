import pytest
from httpx import AsyncClient
from fastapi import status

# When using pytest the fixtures need to be added as arguments to the test functions, Pytest injects them

@pytest.mark.asyncio
async def test_create_ehr_without_body_success(client: AsyncClient):
    """
    Test POST /ehr: Successfully create an EHR with no request body
    """

    # Run the request
    response = await client.post("/v1/ehr")

    # Assert the response
    assert response.status_code == status.HTTP_201_CREATED

    # Check the headers
    assert "Location" in response.headers
    assert "ETag" in response.headers
    assert "Last-Modified" in response.headers

    # Check body content
    data = response.json()
    assert "ehr_id" in data
    assert data["ehr_status"]["subject"]["namespace"] == "system.unassigned"

@pytest.mark.asyncio
async def test_create_ehr_with_subject_success(client: AsyncClient):
    """
    Test POST /ehr: Successfully create an EHR with a subject in the body
    """

    ehr_status_payload = {
        "subject": {
            "id": "test-subject-123",
            "namespace": "test.namespace"
        }
    }

    response = await client.post("/v1/ehr", json = ehr_status_payload)
    assert response.status_code == status.HTTP_201_CREATED

    data = response.json()
    assert data["ehr_status"]["subject"]["id"] == "test-subject-123"
    assert data["ehr_status"]["subject"]["namespace"] == "test.namespace"


@pytest.mark.asyncio
async def test_create_ehr_conflict(client: AsyncClient):
    """
    Test POST /ehr: Ensure a 409 Conflict is returned if an EHR for a sbuject already exists
    """

    ehr_status_payload = {
        "subject": {
            "id": "conflict-subject-456",
            "namespace": "test.namespace"
        }
    }

    # Create the first EHR
    response1 = await client.post("/v1/ehr", json=ehr_status_payload)
    assert response1.status_code == status.HTTP_201_CREATED

    # Attempt to create the same EHR resource
    response2 = await client.post("/v1/ehr", json = ehr_status_payload)
    assert response2.status_code == status.HTTP_409_CONFLICT
    assert "already exists" in response2.json()["detail"]


@pytest.mark.asyncio
async def test_get_ehr_by_id_success(client: AsyncClient):
    """
    Test GET /ehr/{ehr_id}: Successfully retrieve an existing EHR
    """
    # First create an EHR to retrieve
    create_response = await client.post("/v1/ehr")
    assert create_response.status_code == status.HTTP_201_CREATED
    ehr_id = create_response.json()["ehr_id"]

    # Retrieve the ehr_id from the EHR GET API
    get_response = await client.get(f"/v1/ehr/{ehr_id}")

    assert get_response.status_code == status.HTTP_200_OK
    data = get_response.json()
    assert data["ehr_id"] == ehr_id
    assert "ehr_status" in data


@pytest.mark.asyncio
async def test_get_ehr_by_id_not_found(client: AsyncClient):
    """
    Test GET /ehr/{ehr_id}: Ensure 404 is returned for a non-existen EHR
    """

    non_existent_ehr_id = "00000000-0000-0000-0000-000000000000"

    response = await client.get(f"/v1/ehr/{non_existent_ehr_id}")
    assert response.status_code == status.HTTP_404_NOT_FOUND


@pytest.mark.asyncio
async def test_create_composition_success(client: AsyncClient):
    """
    Test POST /ehr/{ehr_id}/composition: Successfully create a composition.
    """
    # Create an EHR to host the composition
    create_ehr_response = await client.post("/v1/ehr")
    assert create_ehr_response.status_code == status.HTTP_201_CREATED
    ehr_id = create_ehr_response.json()["ehr_id"]

    # Define a valid composition payload
    composition_payload = {
        "_type": "COMPOSITION",
        "archetype_details": {
            "template_id": {
                "value": "Test-Template"
            }
        },
        "name": {
            "value": "Test Composition"
        },
        "content": []
    }

    # POST the composition
    response = await client.post(f"/v1/ehr/{ehr_id}/composition", json=composition_payload)

    # Assert the result
    assert response.status_code == status.HTTP_201_CREATED
    assert "Location" in response.headers
    assert "ETag" in response.headers
    data = response.json()
    assert "uid" in data
    assert data["data"]["name"]["value"] == "Test Composition"