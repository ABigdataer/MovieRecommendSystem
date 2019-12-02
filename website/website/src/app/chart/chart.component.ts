import { Component, OnInit } from '@angular/core';
import {LoginService} from '../services/login.service';
import {constant} from '../model/constant';
import {Http} from '@angular/http';

@Component({
  selector: 'app-chart',
  templateUrl: './chart.component.html',
  styleUrls: ['./chart.component.css']
})
export class ChartComponent implements OnInit {

  constructor(public httpService: Http, public loginService: LoginService) { }

  option = {}

  ngOnInit() {
    this.httpService
      .get(constant.BUSSINESS_SERVER_URL + 'rest/movie/stat?username=' + this.loginService.user.username)
      .subscribe(
        data => {
          if(data['success'] === true) {
            this.option = {
              title: {
                text: '个人评分趋势'
              },
              color: ['#3398DB'],
              tooltip : {
                trigger: 'axis',
                axisPointer : {
                  type : 'shadow'
                }
              },
              grid: {
                left: '3%',
                right: '4%',
                bottom: '3%',
                containLabel: true
              },
              xAxis : [
                {
                  type : 'category',
                  data : ['0.5', '1', '1.5', '2', '2.5', '3', '3.5', '4', '4.5', '5'],
                  axisTick: {
                    alignWithLabel: true
                  }
                }
              ],
              yAxis : [
                {
                  type : 'value'
                }
              ],
              series : [
                {
                  name: '电影个数',
                  type: 'bar',
                  barWidth: '60%',
                  data: data['stat']
                }
              ]
            };
          }
        },
        err => {
          console.log('Somethi,g went wrong!');
        }
      );
  }
}
