
/* global bootstrap: false */
(function () {
  /*'use strict'
  var tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'));
  tooltipTriggerList.forEach(function (tooltipTriggerEl) {
    new bootstrap.Tooltip(tooltipTriggerEl)
  });*/
  feather.replace();
})()

function toggleTTL() {
  let timeToLiveContainer = document.getElementById("timeToLiveContainer");
  if (timeToLiveContainer.style.display === "none") {
      timeToLiveContainer.style.display = "block";
  } else {
      timeToLiveContainer.style.display = "none";
  }
}

