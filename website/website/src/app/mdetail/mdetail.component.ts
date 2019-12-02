import { Component, OnInit } from '@angular/core';
import {Movie} from "../model/movie";
import {HttpClient} from "@angular/common/http";
import {LoginService} from "../services/login.service";
import {Tag} from "../model/tag";
import {ActivatedRoute, ParamMap} from "@angular/router";
import 'rxjs/add/operator/switchMap';
import {constant} from "../model/constant";

@Component({
  selector: 'app-mdetail',
  templateUrl: './mdetail.component.html',
  styleUrls: ['./mdetail.component.css']
})
export class MdetailComponent implements OnInit {

  movie: Movie = new Movie;
  sameMovies: Movie[] = [];
  myTags: Tag[] = [];
  movieTags: Tag[] = [];

  imageServer = constant.IMAGE_SERVER_URL;

  constructor(
    private route:ActivatedRoute,
    private httpService : HttpClient,
    private loginService: LoginService
  ) {}

  ngOnInit(): void {

    this.route.params
      .subscribe(params => {
        var id = params['id'];
        this.getMovieInfo(id);
        this.getSameMovies(id);
        this.getMyTags(id);
        this.getMovieTags(id);
    });
  }

  addMyTag(id:number, name:string):void{
    this.httpService
      .get(constant.BUSSINESS_SERVER_URL+'/rest/movie/newtag/'+ id +'?username='+this.loginService.user.username+'&tagname='+name)
      .subscribe(
        data => {
          if(data['success'] == true){
            this.myTags.push(data['tag']);
          }
        },
        err => {
          console.log('Somethi,g went wrong!');
        }
      );
  }


  removeMyTag(id:number):void {
    /*for (let myTag of this.myTags) {
      if(myTag.id == id){
        this.myTags.unshift(myTag);
        break;
      }
    }*/
  }
  getMyTags(id:number):void{
    this.httpService
      .get(constant.BUSSINESS_SERVER_URL+'rest/movie/mytag/'+id+'?username='+this.loginService.user.username)
      .subscribe(
        data => {
          if(data['success'] == true){
            this.myTags = data["tags"]
          }
        },
        err => {
          console.log('Somethi,g went wrong!');
        }
      );
  }


  getMovieTags(id:number):void{
    this.httpService
      .get(constant.BUSSINESS_SERVER_URL+'/rest/movie/tag/'+id)
      .subscribe(
        data => {
          if(data['success'] == true){
            this.movieTags = data['tags'];
          }
        },
        err => {
          console.log('Somethi,g went wrong!');
        }
      );
  }


  getSameMovies(id:number):void{
    this.httpService
      .get(constant.BUSSINESS_SERVER_URL+'/rest/movie/same/'+id+'?num=6')
      .subscribe(
        data => {
          if(data['success'] == true){
            this.sameMovies = data['movies']
          }
        },
        err => {
          console.log('Somethi,g went wrong!');
        }
      );
  }

  getMovieInfo(id:number):void{
    this.httpService
      .get(constant.BUSSINESS_SERVER_URL+'/rest/movie/info/'+id)
      .subscribe(
        data => {
          if(data['success'] == true){
            this.movie = data['movie'];
          }
        },
        err => {
          console.log('Somethi,g went wrong!');
        }
      );
  }
}
