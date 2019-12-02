import { NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';

import {HomeComponent} from "./home/home.component";
import {LoginComponent} from "./login/login.component";
import {RegisterComponent} from "./register/register.component";
import {MdetailComponent} from "./mdetail/mdetail.component";
import {ExploreComponent} from "./explore/explore.component";
import {TagsComponent} from "./tags/tags.component";
import {ChartComponent} from "./chart/chart.component";

const routes: Routes = [
  { path: '', redirectTo: '/home', pathMatch: 'full' },
  { path: 'movies/:id', component: MdetailComponent },
  { path: 'home',     component: HomeComponent },
  { path: 'login',     component: LoginComponent },
  { path: 'register',     component: RegisterComponent },
  { path: 'tags', component: TagsComponent},
  { path: 'explore',     component: ExploreComponent },
  { path: 'chart',     component: ChartComponent }
];

@NgModule({
  imports: [ RouterModule.forRoot(routes) ],
  exports: [ RouterModule ]
})
export class AppRoutingModule {}
