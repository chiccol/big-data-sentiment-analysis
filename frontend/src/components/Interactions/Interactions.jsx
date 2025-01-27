import React, { useState, useEffect } from "react";
import Chart from "react-apexcharts";

const Interactions = ({ company }) => {
  // Maintain chart data in local state
  const [chartData, setChartData] = useState({
    series: [
      {
        name: "Interactions",
        data: [],
      },
    ],
    options: {
      chart: {
        type: "area",
        height: "auto",
      },
      fill: {
        colors: ["#fff"],
        type: "gradient",
      },
      dataLabels: {
        enabled: false,
      },
      stroke: {
        curve: "smooth",
        colors: ["#ff929f"],
      },
      tooltip: {
        x: {
          format: "dd/MM/yy HH:mm",
        },
      },
      grid: {
        show: false,
      },
      xaxis: {
        type: "datetime",
        categories: [],
      },
      yaxis: {
        show: false,
      },
      toolbar: {
        show: false,
      },
    },
  });

  // Fetch data on component mount or whenever `company` changes
  useEffect(() => {
    if (!company) return; // Avoid fetching if no company is given

    fetch(`http://localhost:8000/interaction-number/${company}`)
      .then((res) => {
        if (!res.ok) {
          throw new Error(`Error fetching interactions for ${company}`);
        }
        return res.json();
      })
      .then((data) => {
        // data should be in the form: { daily_counts: [ { normalized_date, daily_count }, ... ] }
        const { daily_counts } = data;

        // Separate dates (x-axis) and counts (y-axis)
        const categories = daily_counts.map((item) => item.normalized_date);
        const counts = daily_counts.map((item) => item.daily_count);

        // Update chartData with new API results
        setChartData((prevState) => ({
          ...prevState,
          series: [
            {
              ...prevState.series[0],
              data: counts,
            },
          ],
          options: {
            ...prevState.options,
            xaxis: {
              ...prevState.options.xaxis,
              categories: categories,
            },
          },
        }));
      })
      .catch((error) => {
        console.error("Error fetching interactions:", error);
      });
  }, [company]);

  return (
    <div className="Interactions">
      <Chart
        options={chartData.options}
        series={chartData.series}
        type="area"
      />
    </div>
  );
};

export default Interactions;
