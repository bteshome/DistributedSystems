
(function () {
    let deleteCartItemForms = document.getElementsByClassName("deleteCartItem");

    for (let i = 0; i < deleteCartItemForms.length; i++) {
        deleteCartItemForms[i].addEventListener("submit", function (event) {
            event.preventDefault();
            const form = this;
            const formData = new FormData(form);

            fetch(form.action, {
                method: 'POST',
                body: formData,
            })
            .then(response => response.json())
            .then(data => {
                form.parentNode.parentNode.remove();
            })
            .catch(error => {
                console.error('Error:', error);
            });
        });
    }
})()
