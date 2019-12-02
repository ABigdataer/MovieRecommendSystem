import { Component, OnInit } from '@angular/core';
import {LoginService} from "../services/login.service";
import {HttpClient} from "@angular/common/http";
import {Tag} from "../model/tag";
import {constant} from "../model/constant";

@Component({
  selector: 'app-tags',
  templateUrl: './tags.component.html',
  styleUrls: ['./tags.component.css']
})
export class TagsComponent implements OnInit {

  constructor(public loginService:LoginService,public httpService:HttpClient) { }

  tags:Tag[] = [];

  ngOnInit() {
    this.httpService
      .get(constant.BUSSINESS_SERVER_URL+'rest/movie/myrate?username='+this.loginService.user.username)
      .subscribe(
        data => {
          if(data['success'] == true){
            this.tags = data['tags'];
          }
        },
        err => {
          console.log('Somethi,g went wrong!');
        }
      );
  }

}
