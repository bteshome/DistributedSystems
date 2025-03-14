import { Component, inject, OnInit, signal } from '@angular/core';
import { CartItem } from '../model/cartItem.type';
import { CartService } from '../services/cart.service';
import { CookieService } from '../services/cookie.service';
import { ApiService } from '../services/api.service';
import { OrderCreateRequest } from '../model/orderCreateRequest.type';
import { AuthService } from '../services/auth.service';
import { catchError } from 'rxjs';
import { OrderCreateResponse } from '../model/orderCreateResponse.type';

@Component({
  selector: 'app-order-create',
  imports: [],
  templateUrl: './order-create.component.html',
  styleUrl: './order-create.component.css'
})

export class OrderCreateComponent implements OnInit {
  page = signal('Place Order');
  cart = signal<Array<CartItem>>([]);
  cartService = inject(CartService);
  cookieService = inject(CookieService);
  apiService = inject(ApiService);
  authService = inject(AuthService);
  user = this.authService.user;
  processing = signal<boolean>(false);
  errorMessage = signal<string>("");
  infoMessage = signal<string>("");

  removeFromCart(cartItem: CartItem) {
    let updatedArray = [...this.cart()];
    updatedArray = updatedArray.filter(item => item.skuCode !== cartItem.skuCode);
    this.cart.set(updatedArray);    
    if (updatedArray.length == 0)
      this.cookieService.setCookie('cart', "", -1, "/"); 
    else
      this.cookieService.setCookie('cart', updatedArray.join("|"), 1, "/"); 
  }

  createOrder() {
    let userObject = this.user();

    if (!userObject) {
      this.authService.signin();
    } else {
      this.processing.set(true);
      this.errorMessage.set("");    
      this.infoMessage.set("");

      let orderCreateRequest: OrderCreateRequest = {
        username: userObject.preferred_username,
        firstName: userObject.given_name,
        lastName: userObject.family_name,
        email: userObject.email,
        lineItems: this.cart()
      }

      this.apiService.createOrder(orderCreateRequest)
        .pipe(
          catchError((error, response) => {
            this.processing.set(false);
            this.errorMessage.set(error.error.error);
            return response;
          })
        )
        .subscribe((response) => {
          this.processing.set(false);
          if (response.httpStatus == 200) {
            this.infoMessage.set(response?.infoMessage);
            this.cart.set([]);
            this.cookieService.setCookie('cart', "", -1, "/"); 
          }
          else {
            this.errorMessage.set(response?.errorMessage);
          }
        });
    }
  }

  ngOnInit(): void {
    this.cart.set(this.cartService.getFromCookie());
  }
}
