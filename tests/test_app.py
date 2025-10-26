from fastapi.testclient import TestClient
from src.app import app, activities


client = TestClient(app)


def test_get_activities():
    resp = client.get("/activities")
    assert resp.status_code == 200
    data = resp.json()
    # Should return a dict with known activities
    assert isinstance(data, dict)
    assert "Chess Club" in data


def test_signup_and_duplicate_rejection():
    activity = "Chess Club"
    email = "tester@example.com"

    # Ensure not present initially
    if email in activities[activity]["participants"]:
        activities[activity]["participants"].remove(email)

    # Signup with JSON body
    resp = client.post(f"/activities/{activity}/signup", json={"email": email})
    assert resp.status_code == 200
    assert resp.json()["message"].startswith("Signed up")

    # Now duplicate signup should fail
    resp2 = client.post(f"/activities/{activity}/signup", json={"email": email})
    assert resp2.status_code == 400
    assert "already signed up" in resp2.json().get("detail", "") or "already signed up" in resp2.json().get("message", "")


def test_remove_participant():
    activity = "Chess Club"
    email = "deleteme@example.com"

    # Ensure participant exists
    if email not in activities[activity]["participants"]:
        activities[activity]["participants"].append(email)

    resp = client.delete(f"/activities/{activity}/participants/{email}")
    assert resp.status_code == 200
    assert "Removed" in resp.json().get("message", "")
    # Ensure removed in-memory
    assert email not in activities[activity]["participants"]
