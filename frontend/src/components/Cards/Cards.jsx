import React, { useState, useEffect } from "react";
import "./Cards.css";
import Card from "../Card/Card"; // Card remains modular
import { fetchDashboardData } from "../../Data/Data";

const Cards = ({ companyName }) => {
  const [cardData, setCardData] = useState({});
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    const loadData = async () => {
      try {
        const data = await fetchDashboardData(companyName);
        setCardData(data || {}); // Ensure `data` is valid
      } catch (err) {
        setError(err.message);
      } finally {
        setLoading(false);
      }
    };

    loadData();
  }, [companyName]);

  if (loading) {
    return <div className="cards">Loading...</div>;
  }

  if (error) {
    return <div className="cards">Error: {error}</div>;
  }

  const cardConfigurations = [
    {
      title: "Main",
      data: cardData.main,
      color: {
        backGround: "#AA9DEE", // Light green background
        boxShadow: "0px 10px 20px 0px #AA9DEE", // Soft green box shadow
        fill: "#6951E1",
        stroke: "#6951E1",
      },
      icon: () => <span>📊</span>,
    },
    {
      title: "Reddit",
      data: cardData.reddit,
      color: {
        backGround: "#FBAF87", // Light orange background
        boxShadow: "0px 10px 20px 0px  #FBAF87", // Soft orange box shadow
        fill: "#F35607",
        stroke: "#F35607",
      },
      icon: () => <span>📘</span>,
    },
    {
      title: "YouTube",
      data: cardData.youtube,
      color: {
        backGround: "#FB878E", // Light red background
        boxShadow: "0px 10px 20px 0px #FB878E", // Soft red box shadow
        fill: "#CB0715",
        stroke: "#CB0715",
      },
      icon: () => <span>🎥</span>,
    },
    {
      title: "Trustpilot",
      data: cardData.trustpilot,
      color: {
        backGround: "#78A9E0", // Light blue background
        boxShadow: "0px 10px 20px 0px  #78A9E0", // Soft blue box shadow
        fill: "#0070C0",
        stroke: "#0070C0",
      },
      icon: () => <span>🌟</span>,
    },
  ];
  

  return (
    <div className="cards">
      {cardConfigurations.map(
        (config, index) =>
          config.data && ( // Render only if data exists
            <Card
              key={index}
              title={config.title}
              data={config.data}
              color={config.color}
              icon={config.icon}
            />
          )
      )}
    </div>
  );
};

export default Cards;
