import { inject, Injectable, signal } from '@angular/core';
import { TokeResponse } from '../model/tokenResponse.type';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { User } from '../model/user.type';
import { Router } from '@angular/router';
import { environment } from '../../environments/environment';
import { CookieService } from '../services/cookie.service';

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
  clientSecret = environment.clientSecret;
  redirectUri = window.location.origin + "/signedin";
  http = inject(HttpClient)
  router = inject(Router);
  cookieService = inject(CookieService);
  tokenResponse = signal<TokeResponse | null>(null);
  user = signal<User | null>(null);

  signin() {
    const redirectUriEncoced = encodeURIComponent(this.redirectUri);
    const authUrl = `${this.authEndpoint}?response_type=code&redirect_uri=${redirectUriEncoced}&client_id=${this.clientId}&client_secret=${this.clientSecret}&scope=openid`;
    window.location.href = authUrl;
  }

  exchangeCodeForTokens(code: string): void {
    const headers = new HttpHeaders({
      'Content-Type': 'application/x-www-form-urlencoded'
    });

    const body = new URLSearchParams();
    body.set('client_id', this.clientId);
    body.set('client_secret', this.clientSecret);
    body.set('grant_type', 'authorization_code');
    body.set('code', code);
    body.set('redirect_uri', this.redirectUri);

    this.http.post<TokeResponse>(this.tokenEndpoint, body.toString(), { headers })
      .subscribe((response: TokeResponse) => {
        this.tokenResponse.set(response);
        this.getUserInfo();
        this.router.navigate(['/']);
      });
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

  // TODO - logout is being blocked by CORS, unlike the token endpoint
  signout() : void {
    const headers = new HttpHeaders({
      'Content-Type': 'application/x-www-form-urlencoded',
      'Authorization': 'Bearer ' + this.tokenResponse()?.access_token
    });

    const body = new URLSearchParams();
    body.set('client_id', this.clientId);

    let tokenHolder = this.tokenResponse();
    if (tokenHolder) {
      let refresh_token = tokenHolder.refresh_token;
      if (refresh_token) {
        body.set('refresh_token', refresh_token);
      }
    }

    this.http.post(this.endSessionEndpoint, body.toString(), {  })
      .subscribe((response: any) => {
        console.log('i got this response: ' + response);
        this.tokenResponse.set(null);
        this.user.set(null);
        this.router.navigate(['/']);
      });
  }
}
