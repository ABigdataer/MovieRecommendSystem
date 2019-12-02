import {Component, Input, OnInit} from '@angular/core';
import {Movie} from "../model/movie";
import {constant} from "../model/constant";

@Component({
  selector: 'app-thumbnail',
  templateUrl: './thumbnail.component.html',
  styleUrls: ['./thumbnail.component.css']
})
export class ThumbnailComponent implements OnInit {

  detail:boolean = false;

  @Input() movie: Movie = new Movie;

  constructor() {

  }

  ngOnInit() {

  }

  hover():void{
    this.detail = true;
  }

  leave():void{
    this.detail = false;
  }

}
