import { Component, inject, OnInit, signal } from '@angular/core';
import { Product } from '../model/product.type';
import { catchError } from 'rxjs';
import { RouterLink } from '@angular/router';
import { CartItem } from '../model/cartItem.type';
import { CookieService } from '../services/cookie.service';
import { CartService } from '../services/cart.service';
import { ApiService } from '../services/api.service';

@Component({
  selector: 'app-products',
  imports: [RouterLink],
  templateUrl: './products.component.html',
  styleUrl: './products.component.css'
})

export class ProductsComponent implements OnInit {
  page = signal('Available Products');
  apiService = inject(ApiService);
  cookieService = inject(CookieService);
  cartService = inject(CartService);
  products = signal<Array<Product>>([]);
  loading = signal<boolean>(false);
  cart = signal<Array<CartItem>>([]);
  errorMessage = signal<string>("");
  infoMessage = signal<string>("");

  clearCart() {
    this.cart.set([]);
    this.cookieService.setCookie('cart', "", -1, "/"); 
  }

  addToCart(product: Product) {
    let updatedArray = [...this.cart()];
    let existingProduct = updatedArray.filter(item => item.skuCode === product.skuCode);

    if (existingProduct.length == 1) {
      updatedArray.forEach(item => {
        if (item.skuCode === product.skuCode)
          item.quantity = item.quantity + 1;
      })     
    } else {
      let cartItem = new CartItem(product.name, product.skuCode, product.price, 1);
      updatedArray.push(cartItem);
    }

    this.cart.set(updatedArray);
    this.cookieService.setCookie('cart', updatedArray.join("|"), 1, "/"); 
  }

  ngOnInit(): void {
    this.loading.set(true);
    this.errorMessage.set("");
    this.cart.set(this.cartService.getFromCookie());
    
    this.apiService.getProducts()
      .pipe(
        catchError(error => {
          this.loading.set(false);
          this.errorMessage.set(error);
          return [];
        })
      )
      .subscribe((data) => {
        this.loading.set(false);
        this.products.set(data);
      });
  }
}
