import React, { useEffect, useState } from "react";
import "./Updates.css";
import { fetchDashboardData } from "../../Data/Data";

const Updates = ({ companyName }) => {
  const [updates, setUpdates] = useState([]);

  useEffect(() => {
    const loadUpdates = async () => {
      try {
        // Fetch the data
        const data = await fetchDashboardData(companyName);

        // Assuming `reddit`, `trustpilot`, and `youtube` have the updates you need
        const allUpdates = [
          ...data.reddit,
          ...data.trustpilot,
          ...data.youtube,
        ];

        setUpdates(allUpdates);
      } catch (error) {
        console.error("Error loading updates:", error);
      }
    };

    loadUpdates();
  }, [companyName]);

  return (
    <div className="Updates">
      {updates.map((update, index) => (
        <div className="update" key={index}>
          <img src={update.img || "placeholder.jpg"} alt="profile" />
          <div className="noti">
            <div style={{ marginBottom: "0.5rem" }}>
              <span>{update.name}</span>
              <span> {update.noti}</span>
            </div>
            <span>{update.time}</span>
          </div>
        </div>
      ))}
    </div>
  );
};

export default Updates;
