import pytest
from httpx import AsyncClient
from fastapi import status

# TODO: Create a file with the static variables, list and dictionaries, such as this one.
VALID_COMPOSITION = {
    "_type": "COMPOSITION",
    "archetype details": {
        "template_id": {
            "value": "Test-Template-v1"
        }
    },
    "name": {
        "value": "Test Composition Version 1"
    },
    "content": [
        {
            "data": "lorem ipsum dolor sit amet"
        }
    ]
}

# When using pytest the fixtures need to be added as arguments to the test functions, Pytest injects them

@pytest.mark.asyncio
async def test_get_ehr_list_success(client: AsyncClient):
    """
    Test  GET /ehr: Retrieves a list of 50 EHR resources sorted by `time_created` in descending order
    """

    # Create two EHRs to ensure the list is not empty
    await client.post("/v1/ehr")
    await client.post("/v1/ehr")

    # Run the request
    response = await client.get("/v1/ehr")

    # Assert the response
    assert response.status_code == status.HTTP_200_OK

    data = response.json()
    
    # Make sure that the result is a list
    assert isinstance(data, list)
    assert len(data) > 0
    assert "ehr_id" in data[0]


@pytest.mark.asyncio
async def test_get_ehr_list_empty(client: AsyncClient):
    """
    Test GET /ehr: Ensure an empty list is returned when no EHRs exist
    """
    response = await client.get("/v1/ehr")
    assert response.status_code == status.HTTP_200_OK
    assert response.json() == []


@pytest.mark.asyncio
async def test_update_ehr_status_success(client: AsyncClient):
    """
    Test PUT /ehr
    """
    # Create an EHR
    create_response = await client.post("/v1/ehr")
    assert create_response.status_code == status.HTTP_201_CREATED

    data = create_response.json()
    ehr_id = data["ehr_id"]
    original_status_uid = data["ehr_status"]["uid"]

    # Prepare the update payload and headers
    update_payload = {
        "subject": data["ehr_status"]["subject"],
        "is_modifiable": False,
        "is_queryable": False
    }

    headers = {"If-Match": f'"{original_status_uid}'}

    # Send the PUT request
    update_response = await client.put(f"/v1/ehr/{ehr_id}/ehr_status", json = update_payload, headers = headers)

    # Assert the response
    assert update_response.status_code == status.HTTP_200_OK
    new_status_data = update_response.json()
    assert new_status_data["is_modifiable"] is False
    assert new_status_data["is_queryable"] is False
    # UID must be updated to a new version
    assert new_status_data["uid"] != original_status_uid


@pytest.mark.asyncio
async def test_update_ehr_status_precondition_failed(client: AsyncClient):
    """
    Test PUT /ehr/{ehr_id}/ehr_status: Fail with 412 if If-Match header is incorrect.
    """

    # Create an EHR
    create_response = await client.post("/v1/ehr")
    assert create_response.status_code == status.HTTP_201_CREATED
    data = create_response.json()
    ehr_id = data["ehr_id"]

    # Use a wrong ETag in the If-Match header
    update_payload = {
        "subject": data["ehr_status"]["subject"],
        "is_modifiable": False
    }

    headers = {
        "If-Match": '"wrong-uid::server::1"'
    }

    # Send request and assert failure
    update_response = await client.put("/v1/ehr/{ehr_id}/ehr_status", json = update_payload, headers=headers)
    assert update_response.status_code == status.HTTP_412_PRECONDITION_FAILED


# Composition endpoints
@pytest.mark.asyncio
async def test_create_composition_success(client: AsyncClient):
    """
    Test POST /ehr/{ehr_id}/composition: Successfully create a composition
    """

    # Create an EHR to host the composition
    create_ehr_response = await client.post("/v1/ehr")
    assert create_ehr_response.status_code == status.HTTP_201_CREATED

    ehr_id = create_ehr_response.json()["ehr_id"]

    # Create the composition
    response = await client.post(f"/v1/ehr/{ehr_id}/composition", json = VALID_COMPOSITION)

    # Assert the result
    assert response.status_code == status.HTTP_201_CREATED
    assert "Location" in response.headers
    assert "ETag" in response.headers
    data = response.json()
    assert "uid" in data
    assert data["data"]["name"]["value"] == "Test Composition Version 1"


@pytest.mark.asyncio
async def test_get_composition_by_version_uid_success(client: AsyncClient):
    """
    Test GET /ehr/{ehr_id}/composition/{version_uid}: Successfully retrieve a composition.
    """

    # Create EHR and Composition
    ehr_response = await client.post("/v1/ehr")
    ehr_id = ehr_response.json()["ehr_id"]

    comp_response = await client.post(f"/v1/ehr/{ehr_id}/composition", json = VALID_COMPOSITION)
    comp_uid = comp_response.json()["uid"]

    # Retrieve the composition
    get_response = await client.get(f"/v1/ehr/{ehr_id}/composition/{comp_uid}")

    # Assert success
    assert get_response.status_code == status.HTTP_200_OK
    assert get_response.headers["ETag"] == f'"{comp_uid}"'
    retrieved_data = get_response.json()
    assert retrieved_data["name"]["value"] == "Test Composition Version 1"


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