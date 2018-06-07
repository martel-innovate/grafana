package crate

import (
	"context"
	_ "path"
	"strconv"
	"fmt"
	_ "strings"
	_ "encoding/json"
	_ "io/ioutil"
	_ "net/http"
	_ "net/url"

	"github.com/grafana/grafana/pkg/components/null"
	"github.com/grafana/grafana/pkg/log"
	"github.com/grafana/grafana/pkg/models"
	_ "github.com/grafana/grafana/pkg/setting"
	"github.com/grafana/grafana/pkg/tsdb"
	"database/sql"
	_ "github.com/herenow/go-crate"
)

type CrateExecutor struct {
}

func NewCrateExecutor(datasource *models.DataSource) (tsdb.TsdbQueryEndpoint, error) {
	return &CrateExecutor{}, nil
}

var (
	plog log.Logger
)

func init() {
	plog = log.New("tsdb.crate")
	tsdb.RegisterTsdbQueryEndpoint("crate-datasource", NewCrateExecutor)
}

func (e *CrateExecutor) Query(ctx context.Context, dsInfo *models.DataSource, queryContext *tsdb.TsdbQuery) (*tsdb.Response, error) {
	dsJson, err := dsInfo.JsonData.Map()
	if err != nil {
		plog.Info("error", err)
	}
	fmt.Println("Alert Context: ", ctx)
	fmt.Println("Alert DSInfo: ", dsInfo)
	fmt.Println("Alert DSJson: ", dsJson)

	result := &tsdb.Response{}

	start := queryContext.TimeRange.GetFromAsMsEpoch()
	startTime := strconv.FormatInt(start, 10)

	end := queryContext.TimeRange.GetToAsMsEpoch()
	endTime := strconv.FormatInt(end, 10)

	db, err := sql.Open("crate", dsInfo.Url)
	if err != nil {
		plog.Info("error", err)
	}

	queryResults := make(map[string]*tsdb.QueryResult)

	timeColumn := dsJson["timeColumn"].(string)
	schema := dsJson["schema"].(string)
	table := dsJson["table"].(string)

	for _, query := range queryContext.Queries {
		q, err := query.Model.Map()
		if err != nil {
			plog.Info("error", err)
		}
		fmt.Println("Alert Query: ", q)

		refID := q["refId"].(string)

		m, err := query.Model.Get("metricAggs").Array()
		if err != nil {
			plog.Info("error", err)
		}

		whereClauses, err := query.Model.Get("whereClauses").Array()
		if err != nil {
			plog.Info("error", err)
		}

		groupByColumns, err := query.Model.Get("groupByColumns").Array()
		if err != nil {
			plog.Info("error", err)
		}

		queryRes := tsdb.NewQueryResult()

		for i, _ := range m {
			metricColumn := m[i].(map[string]interface{})["column"].(string)
			metricType := m[i].(map[string]interface{})["type"].(string)
			queryString := ""
			if metricType == "raw" {
				queryString = "SELECT "+timeColumn+","+metricColumn+" FROM "+schema+"."+table+" WHERE "+timeColumn+">"+startTime+" AND "+timeColumn+"<"+endTime
			} else {
				queryString = "SELECT "+timeColumn+","+metricType+"("+metricColumn+")"+" FROM "+schema+"."+table+" WHERE "+timeColumn+">"+startTime+" AND "+timeColumn+"<"+endTime
			}

			for i, _ := range whereClauses {
				c := whereClauses[i].(map[string]interface{})
				cond := "AND"
				if c["condition"].(string) != "" {
					cond = c["condition"].(string)
				}
				queryString = queryString+" "+cond+" "+c["column"].(string)+c["operator"].(string)+c["value"].(string)
			}
			if len(groupByColumns) == 0 && metricType != "raw" {
				queryString = queryString+" GROUP BY "+timeColumn
			}
			if len(groupByColumns) != 0 {
				queryString = queryString+" GROUP BY "+timeColumn
				if metricType == "raw" {
					queryString = queryString+","+metricColumn
				}
				for i, _ := range groupByColumns {
					if groupByColumns[i].(string) != timeColumn {
						queryString = queryString+","+groupByColumns[i].(string)
					}
				}
			}
			queryString = queryString+";"
			fmt.Println("Alert Query String: ", queryString)

			rows, err := db.Query(queryString)
			if err != nil {
				plog.Info("error", err)
				continue
			}
			defer rows.Close()

			series := tsdb.TimeSeries{
				Name: metricColumn,
			}

			for rows.Next() {
				var timeValue string
				var metricValue string

				if err := rows.Scan(&timeValue, &metricValue); err != nil {
					plog.Info("error", err)
				}

				floatTimeValue, err := strconv.ParseFloat(timeValue, 64)
				if err != nil {
					plog.Info("error", err)
				}

				floatMetricValue, err := strconv.ParseFloat(metricValue, 64)
				if err != nil {
					plog.Info("error", err)
				}

				series.Points = append(series.Points, tsdb.NewTimePoint(null.FloatFrom(floatMetricValue), floatTimeValue))
			}
			if err := rows.Err(); err != nil {
				plog.Info("error", err)
			}
			queryRes.Series = append(queryRes.Series, &series)
		}
		queryResults[refID] = queryRes
	}
	result.Results = queryResults
	return result, nil
}
