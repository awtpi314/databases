document.addEventListener("DOMContentLoaded", function () {
  const battingStatsTable = document.getElementById("batting-stats-table");
  const homeRunsTable = document.getElementById("home-runs-table");
  const eraStatsTable = document.getElementById("era-stats-table");

  const loadingElements = document.querySelectorAll(".loading-indicator");
  loadingElements.forEach((el) => {
    el.textContent = "Loading latest data...";
  });

  fetch("/api/season_stats/")
    .then((response) => {
      if (!response.ok) {
        throw new Error("Network response was not ok");
      }
      return response.json();
    })
    .then((data) => {
      loadingElements.forEach((el) => {
        el.textContent = "";
      });

      updateTable(battingStatsTable, data.batting_stats, ["name", "average"]);

      updateTable(homeRunsTable, data.home_runs, ["name", "home_runs"]);

      updateTable(eraStatsTable, data.era_stats, ["name", "era"]);
    })
    .catch((error) => {
      console.error("Error fetching data:", error);
      loadingElements.forEach((el) => {
        el.textContent = "Error loading data. Please refresh the page.";
      });
    });
});

function updateTable(tableElement, data, columns) {
  if (!tableElement) return;

  const tbody = tableElement.querySelector("tbody");
  tbody.innerHTML = "";

  data.forEach((item) => {
    const row = document.createElement("tr");
    columns.forEach((column) => {
      const cell = document.createElement("td");
      cell.textContent = item[column];
      row.appendChild(cell);
    });
    tbody.appendChild(row);
  });
}
