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
var login_service_1 = require("../services/login.service");
var http_1 = require("@angular/common/http");
var TagsComponent = (function () {
    function TagsComponent(loginService, httpService) {
        this.loginService = loginService;
        this.httpService = httpService;
        this.tags = [];
    }
    TagsComponent.prototype.ngOnInit = function () {
        var _this = this;
        this.httpService
            .get(constant.BUSSINESS_SERVER_URL+'rest/movie/myrate?username=' + this.loginService.user.username)
            .subscribe(function (data) {
            if (data['success'] == true) {
                _this.tags = data['tags'];
            }
        }, function (err) {
            console.log('Somethi,g went wrong!');
        });
    };
    TagsComponent = __decorate([
        core_1.Component({
            selector: 'app-tags',
            templateUrl: './tags.component.html',
            styleUrls: ['./tags.component.css']
        }),
        __metadata("design:paramtypes", [login_service_1.LoginService, http_1.HttpClient])
    ], TagsComponent);
    return TagsComponent;
}());
exports.TagsComponent = TagsComponent;
//# sourceMappingURL=tags.component.js.map
