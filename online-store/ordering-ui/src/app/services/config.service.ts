import { inject, Injectable } from '@angular/core';
import { environment } from '../../environments/environment';
import { HttpClient } from '@angular/common/http';
import { ConfigGetRequest } from '../model/configGetRequest.type';
import { ConfigGetResponse } from '../model/configGetResponse.type';

@Injectable({
  providedIn: 'root'
})

export class ConfigService {
  baseUrl = environment.apiBaseUrl;
  configUrl: string = this.baseUrl + "/ordering-ui/config/"

  http = inject(HttpClient);

  getClientSecret(){
    let configGetRequest: ConfigGetRequest = {
      name: "clientsecret"
    }
    return this.http.post<ConfigGetResponse>(this.configUrl, configGetRequest);
  }
}
