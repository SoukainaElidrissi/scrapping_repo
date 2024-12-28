import { Component, OnInit } from '@angular/core';
import { ApiService } from './api.service';

@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.css']
})
export class AppComponent implements OnInit {
  data: any;

  constructor(private apiService: ApiService) {}

  ngOnInit(): void {
    this.apiService.getChartData().subscribe((response: any[]) => {
      console.log('Chart Data:', response);
    });
  }
}