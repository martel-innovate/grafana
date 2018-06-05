package crate

import (
	"context"
	"fmt"
	_ "path"
	"strconv"
	_ "strings"

	_ "golang.org/x/net/context/ctxhttp"

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
	fmt.Println("Context: ", ctx)
	fmt.Println("DSInfo: ", dsInfo)
	dsJson, err := dsInfo.JsonData.Map()
	if err != nil {
		plog.Info("error", err)
	}
	fmt.Println("DSJson: ", dsJson)
	result := &tsdb.Response{}

	start := queryContext.TimeRange.GetFromAsMsEpoch()
	startTime := strconv.FormatInt(start, 10)
	fmt.Println("Start: ", start)
	end := queryContext.TimeRange.GetToAsMsEpoch()
	endTime := strconv.FormatInt(end, 10)
	fmt.Println("End: ", end)

	db, err := sql.Open("crate", dsInfo.Url)
	if err != nil {
		plog.Info("error", err)
	}

	queryResults := make(map[string]*tsdb.QueryResult)

	for _, query := range queryContext.Queries {
		q, err := query.Model.Map()
		if err != nil {
			plog.Info("error", err)
		}
		m, err := query.Model.Get("metricAggs").Array()
		if err != nil {
			plog.Info("error", err)
		}
		metricColumn := m[0].(map[string]interface{})["column"].(string)
		timeColumn := dsJson["timeColumn"].(string)
		schema := dsJson["schema"].(string)
		table := dsJson["table"].(string)
		refID := q["refId"].(string)
		fmt.Println("Full Query: ", q)
		fmt.Println("Query: ", m[0].(map[string]interface{})["column"])
		rows, err := db.Query("SELECT "+timeColumn+","+metricColumn+" FROM "+schema+"."+table+" WHERE "+timeColumn+">"+startTime+" AND "+timeColumn+"<"+endTime+";")
		if err != nil {
			plog.Info("error", err)
		}
		defer rows.Close()
		queryRes := tsdb.NewQueryResult()
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
		queryResults[refID] = queryRes
	}

	result.Results = queryResults
	return result, nil
}
