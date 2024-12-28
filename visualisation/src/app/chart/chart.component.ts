import { Component, Inject, NgZone, OnInit, PLATFORM_ID } from '@angular/core';
import { isPlatformBrowser } from '@angular/common';
import { ApiService } from '../api.service';
import * as am5 from '@amcharts/amcharts5';
import * as am5xy from '@amcharts/amcharts5/xy';
import am5themes_Animated from '@amcharts/amcharts5/themes/Animated';

@Component({
  selector: 'app-chart',
  templateUrl: './chart.component.html',
  styleUrls: ['./chart.component.css'],
})
export class ChartComponent implements OnInit {
  private root!: am5.Root;

  constructor(
    @Inject(PLATFORM_ID) private platformId: Object,
    private zone: NgZone,
    private apiService: ApiService
  ) {}

  ngOnInit(): void {
    this.apiService.getChartData().subscribe((data: any[]) => {
      const processedData = this.prepareChartData(data);
      this.createGroupedChart(processedData);
    });
  }

  browserOnly(f: () => void) {
    if (isPlatformBrowser(this.platformId)) {
      this.zone.runOutsideAngular(() => {
        f();
      });
    }
  }

  /**
   * Prépare les données : top 5 articles cités pour chaque année.
   */
  prepareChartData(data: any[]): any[] {
    const groupedByYear = data.reduce((acc: any, item) => {
      const year = item.Year;
      const citations = parseFloat(item.citations);
      if (!acc[year]) {
        acc[year] = [];
      }
      acc[year].push({ ...item, citations });
      return acc;
    }, {});

    // Récupérer les 5 articles les plus cités par année
    const result = Object.keys(groupedByYear).map((year) => {
      const topArticles = groupedByYear[year]
        .sort((a: any, b: any) => b.citations - a.citations)
        .slice(0, 5);

      // Ajouter l'année comme clé
      return {
        year,
        articles: topArticles.map((article: any, index: number) => ({
          name: `Article ${index + 1}`, // Noms génériques
          title: article.title,
          citations: article.citations,
        })),
      };
    });

    return result;
  }

  /**
   * Crée un graphique groupé.
   */
  createGroupedChart(data: any[]): void {
    this.browserOnly(() => {
      let root = am5.Root.new('chartdiv');
      root.setThemes([am5themes_Animated.new(root)]);

      let chart = root.container.children.push(
        am5xy.XYChart.new(root, {
          panX: true,
          panY: false,
          wheelX: 'panX',
          wheelY: 'zoomX',
          layout: root.verticalLayout,
        })
      );

      // Axes
      let xAxis = chart.xAxes.push(
        am5xy.CategoryAxis.new(root, {
          categoryField: 'year',
          renderer: am5xy.AxisRendererX.new(root, {}),
        })
      );

      let yAxis = chart.yAxes.push(
        am5xy.ValueAxis.new(root, {
          min: 0,
          strictMinMax: true,
          renderer: am5xy.AxisRendererY.new(root, {}),
        })
      );

      // Ajouter les séries (une série par article)
      data.forEach((yearData) => {
        yearData.articles.forEach((article: any) => {
          let series = chart.series.push(
            am5xy.ColumnSeries.new(root, {
              name: article.name,
              xAxis: xAxis,
              yAxis: yAxis,
              valueYField: 'citations',
              categoryXField: 'year',
              tooltip: am5.Tooltip.new(root, {
                labelText: `[bold]{name}[/]: {citations} citations\n{title}`,
              }),
            })
          );

          // Ajouter les données à la série
          series.data.setAll([
            {
              year: yearData.year,
              citations: article.citations,
              title: article.title,
              name: article.name,
            },
          ]);
        });
      });

      // Légende
      let legend = chart.children.push(am5.Legend.new(root, {}));
      legend.data.setAll(chart.series.values);

      this.root = root;
    });
  }

  ngOnDestroy(): void {
    this.browserOnly(() => {
      if (this.root) {
        this.root.dispose();
      }
    });
  }
}
