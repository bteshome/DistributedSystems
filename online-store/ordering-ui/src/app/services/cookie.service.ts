import { DOCUMENT } from '@angular/common';
import { inject, Injectable } from '@angular/core';

@Injectable({
  providedIn: 'root'
})
export class CookieService {
  document = inject(DOCUMENT);

  constructor() { }

  getCookie(name: string): string | null {
    const nameEQ = name + '=';
    const cookies = this.document.cookie.split(';');
    for (let i = 0; i < cookies.length; i++) {
      let cookie = cookies[i];
      while (cookie.charAt(0) === ' ') {
        cookie = cookie.substring(1, cookie.length);
      }
      if (cookie.indexOf(nameEQ) === 0) {
        return cookie.substring(nameEQ.length, cookie.length);
      }
    }
    return null;
  }

  setCookie(name: string, value: string, days?: number, path?: string): void {
    let expires = '';
    if (days) {
      const date = new Date();
      date.setTime(date.getTime() + (days * 24 * 60 * 60 * 1000));
      expires = '; expires=' + date.toUTCString();
    }
    const cookiePath = path ? '; path=' + path : '; path=/';
    this.document.cookie = `${name}=${value}${expires}${cookiePath}`;
  }

  deleteCookie(name: string): void {
    this.setCookie(name, "", -1, "/");
  }
}
