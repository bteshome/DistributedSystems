import { inject, Injectable, OnInit, signal } from '@angular/core';
import { TokeResponse } from '../model/tokenResponse.type';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { User } from '../model/user.type';
import { Router } from '@angular/router';
import { environment } from '../../environments/environment';
import { CookieService } from '../services/cookie.service';
import { ConfigService } from './config.service';

@Injectable({
  providedIn: 'root'
})

export class AuthService {
  baseUrl = environment.authBaseUrl;
  realm = environment.realm;
  authEndpoint = this.baseUrl + "/realms/" + this.realm + "/protocol/openid-connect/auth";
  tokenEndpoint = this.baseUrl + "/realms/" + this.realm + "/protocol/openid-connect/token"
  userInfoEndpoint = this.baseUrl + "/realms/" + this.realm + "/protocol/openid-connect/userinfo";
  endSessionEndpoint = this.baseUrl + "/realms/" + this.realm + "/protocol/openid-connect/logout";
  clientId = environment.clientId;
  redirectUri = window.location.origin + "/signedin";
  http = inject(HttpClient)
  router = inject(Router);
  cookieService = inject(CookieService);
  configService = inject(ConfigService);
  tokenResponse = signal<TokeResponse | null>(null);
  user = signal<User | null>(null);

  signin() {
    let redirectUriEncoced = encodeURIComponent(this.redirectUri);
    let authUrl = `${this.authEndpoint}?response_type=code&redirect_uri=${redirectUriEncoced}&client_id=${this.clientId}&scope=openid`;
    window.location.href = authUrl;
  }

  exchangeCodeForTokens(code: string): void {
    this.configService.getClientSecret()
      .subscribe((response) => {
        if (response.httpStatus != 200) {
          console.error("Config service call returned a status code of " + response.httpStatus);
          console.error(response.errorMessage);
        } else {
          let clientSecret = response.value;
          this.cookieService.setCookie('cs', clientSecret, 1, "/");

          let headers = new HttpHeaders({
            'Content-Type': 'application/x-www-form-urlencoded'
          });

          let body = new URLSearchParams();
          body.set('client_id', this.clientId);
          body.set('client_secret', clientSecret);
          body.set('grant_type', 'authorization_code');
          body.set('code', code);
          body.set('redirect_uri', this.redirectUri);

          this.http.post<TokeResponse>(this.tokenEndpoint, body.toString(), { headers })
            .subscribe((response: TokeResponse) => {
              this.tokenResponse.set(response);
              this.getUserInfo();
              this.router.navigate(['/']);
            });
        }});
  }

  getUserInfo() {
    const headers = new HttpHeaders ({
      'Authorization': 'Bearer ' + this.tokenResponse()?.access_token
    });

    this.http.get<User | null>(this.userInfoEndpoint, { headers })
      .subscribe((user : User | null) => {
        this.user.set(user);
      });
  }

  signout() : void {
    let clientSecret = this.cookieService.getCookie('cs')

    if (clientSecret == null) {
      console.error("Cannot sign out. Client secret not found in cookie.");
      return;
    }

    const headers = new HttpHeaders({
      'Content-Type': 'application/x-www-form-urlencoded'
    });

    let body = new URLSearchParams();

    body.set('client_id', this.clientId);
    body.set('client_secret', clientSecret);

    let tokenHolder = this.tokenResponse();
    if (tokenHolder) {
      let refresh_token = tokenHolder.refresh_token;
      if (refresh_token) {
        body.set('refresh_token', refresh_token);
      }
    }

    this.http.post(this.endSessionEndpoint, body.toString(), { headers })
      .subscribe((response: any) => {
        this.tokenResponse.set(null);
        this.user.set(null);
        this.cookieService.deleteCookie('cs');
        this.router.navigate(['/']);
      });
  }
}
