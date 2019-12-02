import { Component, OnInit } from '@angular/core';
import {ActivatedRoute, Router} from "@angular/router";
import {Movie} from "../model/movie";
import {LoginService} from "../services/login.service";
import {constant} from "../model/constant";
import {Http} from "@angular/http";

@Component({
  selector: 'app-explore',
  templateUrl: './explore.component.html',
  styleUrls: ['./explore.component.css']
})
export class ExploreComponent implements OnInit {

  constructor(public loginService:LoginService,public router:ActivatedRoute, public httpService:Http) { }

  title:String;

  movies:Movie[]=[];

  ngOnInit() {
    this.router.params.subscribe(params => {
      if(params['type']=="search"){
        this.title = "搜索结果：";
        if(params['query'].trim() != "")
          this.getSearchMovies(params['query']);

      }else if(params['type']== "guess"){
        this.title = "猜你喜欢：";
        this.getGuessMovies();
      }else if(params['type']== "hot"){
        this.title = "热门推荐：";
        this.getHotMovies();
      }else if(params['type']== "new"){
        this.title = "新片发布：";
        this.getNewMovies();
      }else if(params['type']== "rate"){
        this.title = "评分最多：";
        this.getRateMoreMovies();
      }else if(params['type']== "wish"){
        this.title = "我喜欢的：";
        this.getWishMovies();
      }else if(params['type']== "genres"){
        this.title = "影片类别："+ params['category'];
        this.getGenresMovies(params['category']);
      }else if(params['type']== "myrate"){
        this.title = "我的评分电影：";
        this.getMyRateMovies();
      }
    });
  }

  getMyRateMovies():void{
    this.httpService
      .get(constant.BUSSINESS_SERVER_URL+'rest/movie/myrate?username='+this.loginService.user.username)
      .subscribe(
        data => {
          if(data['success'] == true){
            this.movies = data['movies'];
          }
        },
        err => {
          console.log('Somethi,g went wrong!');
        }
      );
  }

  getGenresMovies(category:String):void{
    this.httpService
      .get(constant.BUSSINESS_SERVER_URL+'rest/movie/search?query='+category)
      .subscribe(
        data => {
          if(data['success'] == true){
            this.movies = data['movies'];
          }
        },
        err => {
          console.log('Somethi,g went wrong!');
        }
      );
  }

  getSearchMovies(query:String):void{
    console.log(query)
    this.httpService
      .get(constant.BUSSINESS_SERVER_URL+'rest/movie/search?query='+query)
      .subscribe(
        data => {
          if(data['success'] == true){
            this.movies = data['movies'];
          }
        },
        err => {
          console.log('Somethi,g went wrong!');
        }
      );
  }

  getGuessMovies():void{
    this.httpService
      .get(constant.BUSSINESS_SERVER_URL+'rest/movie/guess?num=100&username='+this.loginService.user.username)
      .subscribe(
        data => {
          if(data['success'] == true){
            this.movies = data['movies'];
          }
        },
        err => {
          console.log('Somethi,g went wrong!');
        }
      );
  }

  getHotMovies():void{
    this.httpService
      .get(constant.BUSSINESS_SERVER_URL+'rest/movie/hot?num=100&username='+this.loginService.user.username)
      .subscribe(
        data => {
          if(data['success'] == true){
            this.movies = data['movies'];
          }
        },
        err => {
          console.log('Somethi,g went wrong!');
        }
      );
  }
  getNewMovies():void{
    this.httpService
      .get(constant.BUSSINESS_SERVER_URL+'rest/movie/new?num=100&username='+this.loginService.user.username)
      .subscribe(
        data => {
          if(data['success'] == true){
            this.movies = data['movies'];
          }
        },
        err => {
          console.log('Somethi,g went wrong!');
        }
      );
  }
  getRateMoreMovies():void{
    this.httpService
      .get(constant.BUSSINESS_SERVER_URL+'rest/movie/rate?num=100&username='+this.loginService.user.username)
      .subscribe(
        data => {
          if(data['success'] == true){
            this.movies = data['movies'];
          }
        },
        err => {
          console.log('Somethi,g went wrong!');
        }
      );
  }
  getWishMovies():void{
    this.httpService
      .get(constant.BUSSINESS_SERVER_URL+'rest/movie/wish?num=100&username='+this.loginService.user.username)
      .subscribe(
        data => {
          if(data['success'] == true){
            this.movies = data['movies'];
          }
        },
        err => {
          console.log('Somethi,g went wrong!');
        }
      );
  }

}
