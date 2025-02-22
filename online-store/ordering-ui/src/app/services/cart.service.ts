import { inject, Injectable } from '@angular/core';
import { CookieService } from './cookie.service';
import { CartItem } from '../model/cartItem.type';

@Injectable({
  providedIn: 'root'
})
export class CartService {
  cookieService = inject(CookieService);

  constructor() { }

  getFromCookie(): CartItem[] {
    let cartItems: CartItem[] = [];
    let cookieValue = this.cookieService.getCookie('cart');
    
    if (cookieValue != null) {
      let itemsSplitted = cookieValue.split("|");      
      for (let index = 0; index < itemsSplitted.length; index++) {
        let cartItem = CartItem.fromString(itemsSplitted[index]);
        cartItems.push(cartItem);
      }
    }

    return cartItems;
  }
}
