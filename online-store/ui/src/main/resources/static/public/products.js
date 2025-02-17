
(function () {
    let addToCartForms = document.getElementsByClassName("addToCart");
    let clearCartButton = document.getElementById("clearCart");

    for (let i = 0; i < addToCartForms.length; i++) {
        addToCartForms[i].addEventListener("submit", function (event) {
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
    }

    clearCartButton.addEventListener("click", function (event) {
        let addToCartForm = document.getElementsByClassName("addToCart")[0];
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
