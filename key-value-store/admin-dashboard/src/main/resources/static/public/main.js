
/* global bootstrap: false */
(function () {
  /*'use strict'
  var tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'));
  tooltipTriggerList.forEach(function (tooltipTriggerEl) {
    new bootstrap.Tooltip(tooltipTriggerEl)
  });*/
  feather.replace();
  toggleLastReadItemKey();
})()

function toggleTTL() {
  let timeToLiveContainer = document.getElementById("timeToLiveContainer");
  if (timeToLiveContainer.style.display === "none") {
      timeToLiveContainer.style.display = "block";
  } else {
      timeToLiveContainer.style.display = "none";
  }
}

function toggleLastReadItemKey() {
  let partitionKey = document.getElementById("partitionKey");
  let lastItemKeyContainer = document.getElementById("lastReadItemKeyContainer");
  if (partitionKey && lastItemKeyContainer) {
      if (partitionKey.value.trim() === "") {
          lastItemKeyContainer.style.display = "none";
      } else {
          lastItemKeyContainer.style.display = "block";
      }
  }
}