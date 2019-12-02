"use strict";
import {constant} from "../model/constant";

var __decorate = (this && this.__decorate) || function (decorators, target, key, desc) {
    var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
    if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
    else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
    return c > 3 && r && Object.defineProperty(target, key, r), r;
};
var __metadata = (this && this.__metadata) || function (k, v) {
    if (typeof Reflect === "object" && typeof Reflect.metadata === "function") return Reflect.metadata(k, v);
};
Object.defineProperty(exports, "__esModule", { value: true });
var core_1 = require("@angular/core");
var http_1 = require("@angular/common/http");
var login_service_1 = require("../services/login.service");
var HomeComponent = (function () {
    function HomeComponent(httpService, loginService) {
        this.httpService = httpService;
        this.loginService = loginService;
        this.guessMovies = [];
        this.hotMovies = [];
        this.newMovies = [];
        this.rateMoreMovies = [];
        this.wishMovies = [];
    }
    HomeComponent.prototype.ngOnInit = function () {
        this.getGuessMovies();
        this.getHotMovies();
        this.getNewMovies();
        this.getRateMoreMovies();
        this.getWishMovies();
    };
    HomeComponent.prototype.getGuessMovies = function () {
        var _this = this;
        this.httpService
            .get(constant.BUSSINESS_SERVER_URL+'rest/movie/guess?num=6&username=' + this.loginService.user.username)
            .subscribe(function (data) {
            if (data['success'] == true) {
                _this.guessMovies = data['movies'];
            }
        }, function (err) {
            console.log('Somethi,g went wrong!');
        });
    };
    HomeComponent.prototype.getHotMovies = function () {
        var _this = this;
        this.httpService
            .get(constant.BUSSINESS_SERVER_URL+'rest/movie/hot?num=6&username=' + this.loginService.user.username)
            .subscribe(function (data) {
            if (data['success'] == true) {
                _this.hotMovies = data['movies'];
            }
        }, function (err) {
            console.log('Somethi,g went wrong!');
        });
    };
    HomeComponent.prototype.getNewMovies = function () {
        var _this = this;
        this.httpService
            .get(constant.BUSSINESS_SERVER_URL+'rest/movie/new?num=6&username=' + this.loginService.user.username)
            .subscribe(function (data) {
            if (data['success'] == true) {
                _this.newMovies = data['movies'];
            }
        }, function (err) {
            console.log('Somethi,g went wrong!');
        });
    };
    HomeComponent.prototype.getRateMoreMovies = function () {
        var _this = this;
        this.httpService
            .get(constant.BUSSINESS_SERVER_URL+'rest/movie/rate?num=6&username=' + this.loginService.user.username)
            .subscribe(function (data) {
            if (data['success'] == true) {
                _this.rateMoreMovies = data['movies'];
            }
        }, function (err) {
            console.log('Somethi,g went wrong!');
        });
    };
    HomeComponent.prototype.getWishMovies = function () {
        var _this = this;
        this.httpService
            .get(constant.BUSSINESS_SERVER_URL+'rest/movie/wish?num=6&username=' + this.loginService.user.username)
            .subscribe(function (data) {
            if (data['success'] == true) {
                _this.wishMovies = data['movies'];
            }
        }, function (err) {
            console.log('Somethi,g went wrong!');
        });
    };
    HomeComponent = __decorate([
        core_1.Component({
            selector: 'app-home',
            templateUrl: './home.component.html',
            styleUrls: ['./home.component.css']
        }),
        __metadata("design:paramtypes", [http_1.HttpClient, login_service_1.LoginService])
    ], HomeComponent);
    return HomeComponent;
}());
exports.HomeComponent = HomeComponent;
//# sourceMappingURL=home.component.js.map
