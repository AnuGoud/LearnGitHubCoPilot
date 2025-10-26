document.addEventListener("DOMContentLoaded", () => {
  const activitiesList = document.getElementById("activities-list");
  const activitySelect = document.getElementById("activity");
  const signupForm = document.getElementById("signup-form");
  const messageDiv = document.getElementById("message");

  // Function to fetch activities from API
  async function fetchActivities() {
    try {
      const response = await fetch("/activities");
      const activities = await response.json();

      // Clear previous content
      activitiesList.innerHTML = "";
      activitySelect.innerHTML = '<option value="">-- Select an activity --</option>';

      Object.entries(activities).forEach(([name, details]) => {
        const activityCard = document.createElement("div");
        activityCard.className = "activity-card";

        const spotsLeft = details.max_participants - details.participants.length;

        // Basic activity info
        const infoHtml = `
          <h4>${name}</h4>
          <p>${details.description}</p>
          <p><strong>Schedule:</strong> ${details.schedule}</p>
          <p><strong>Availability:</strong> ${spotsLeft} spots left</p>
        `;
        activityCard.innerHTML = infoHtml;

        // Participants section
        const participantsSection = document.createElement("div");
        participantsSection.className = "participants-section";

        const header = document.createElement("h5");
        header.innerHTML = `Participants <span class="badge">${details.participants.length}</span>`;
        participantsSection.appendChild(header);

        if (details.participants && details.participants.length > 0) {
          const ul = document.createElement("ul");
          ul.className = "participants-list";
          details.participants.forEach((email) => {
            const li = document.createElement("li");
            li.textContent = email;
            ul.appendChild(li);
          });
          participantsSection.appendChild(ul);
        } else {
          const empty = document.createElement("p");
          empty.className = "info";
          empty.textContent = "No participants yet — be the first to sign up!";
          participantsSection.appendChild(empty);
        }

        activityCard.appendChild(participantsSection);
        activitiesList.appendChild(activityCard);

        // Add option to select dropdown
        const option = document.createElement("option");
        option.value = name;
        option.textContent = name;
        activitySelect.appendChild(option);
      });
    } catch (error) {
      activitiesList.innerHTML = "<p>Failed to load activities. Please try again later.</p>";
      console.error("Error fetching activities:", error);
    }
  }

  // Handle form submission
  signupForm.addEventListener("submit", async (event) => {
    event.preventDefault();

    const email = document.getElementById("email").value;
    const activity = document.getElementById("activity").value;

    try {
      const response = await fetch(`/activities/${encodeURIComponent(activity)}/signup`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ email })
      });

      const result = await response.json();
      if (response.ok) {
        messageDiv.textContent = result.message;
        messageDiv.className = "message success";
        signupForm.reset();

        // Refresh UI to show the new participant
        fetchActivities();
      } else {
        messageDiv.textContent = result.detail || result.message || "Signup failed";
        messageDiv.className = "message error";
      }
    } catch (err) {
      messageDiv.textContent = "Network error, please try again.";
      messageDiv.className = "message error";
    }

    messageDiv.classList.remove("hidden");

    // Hide message after 5 seconds
    setTimeout(() => {
      messageDiv.classList.add("hidden");
    }, 5000);
  });

  // Initialize app
  fetchActivities();
});
