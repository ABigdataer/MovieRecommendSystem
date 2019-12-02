import {Component, Input, OnInit} from '@angular/core';
import {HttpClient} from "@angular/common/http";
import {LoginService} from "../services/login.service";
import {constant} from "../model/constant";

@Component({
  selector: 'app-star',
  templateUrl: './star.component.html',
  styleUrls: ['./star.component.css']
})
export class StarComponent implements OnInit {

  @Input() currentValue : number=0;
  @Input() mid : number=0;
  tempValue : number = null;
  rating:boolean = false;
  setRate:boolean = false;

  stars = [0,1,2,3,4,5,6,7,8,9];

  constructor(public httpService:HttpClient,public loginService:LoginService) { }

  ngOnInit() {
    this.currentValue = this.currentValue * 2
  }

  getState(index:number) : string {
    if(index < this.currentValue){
      return (index%2 ==0)?"scoreBefore":"scoreAfter";
    }
    return "";
  }

  isScoreBefore(index:number): boolean {
    if(this.rating)
      return false;
    if(index < this.currentValue){
      return (index%2 ==0)?true:false;
    }
    return false;
  }

  isScoreAfter(index:number): boolean {
    if(this.rating)
      return false;
    if(index < this.currentValue){
      return (index%2 ==0)?false:true;
    }
    return false;
  }

  isNoScoreBefore(index:number):boolean{
    if(index < this.currentValue){
      return false;
    }
    return (index%2 ==0)?true:false;
  }

  isNoScoreAfter(index:number):boolean{
    if(index < this.currentValue){
      return false;
    }
    return (index%2 ==0)?false:true;
  }

  isRatingBefore(index:number):boolean{
    if(!this.rating)
      return false;
    if(index < this.currentValue){
      return (index%2 ==0)?true:false;
    }
    return false;
  }
  isRatingAfter(index:number):boolean{
    if(!this.rating)
      return false;
    if(index < this.currentValue){
      return (index%2 ==0)?false:true;
    }
    return false;
  }

  hover(index:number):void{
    this.rating = true;
    if(this.tempValue == null)
      this.tempValue = this.currentValue;
    this.currentValue = index+1;

  }

  rate(index:number):void{
    this.setRate = true;
    this.currentValue = index+1;
    this.rating = true;

    this.httpService
      .get(constant.BUSSINESS_SERVER_URL+'rest/movie/rate/'+this.mid+"?score="+this.currentValue+"&username="+this.loginService.user.username)
      .subscribe(
        data => {
          if(data['success'] == true){

          }
        },
        err => {
          console.log('Somethi,g went wrong!');
        }
      );

  }

  leave():void{
    if(!this.setRate){
      this.rating = false;
      this.currentValue = this.tempValue;
      this.tempValue = null;
    }
  }

}
