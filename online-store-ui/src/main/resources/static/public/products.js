
(function () {
    let addToCartForm = document.getElementById("addToCart");
    let clearCartButton = document.getElementById("clearCart");

    addToCartForm.addEventListener("submit", function (event) {
        event.preventDefault();
        const form = this;
        const formData = new FormData(form);

        fetch(form.action, {
            method: 'POST',
            body: formData,
        })
        .then(response => response.json())
        .then(data => {
            document.getElementById('cart').textContent = data.length;
        })
        .catch(error => {
            console.error('Error:', error);
        });
    });

    clearCartButton.addEventListener("click", function (event) {
        let addToCartForm = document.getElementById("addToCart");
        fetch(addToCartForm.action + "clear/", {
            method: 'POST'
        })
        .then(response => response.json())
        .then(data => {
            document.getElementById('cart').textContent = data.length;
        })
        .catch(error => {
            console.error('Error:', error);
        });
    });
})()
