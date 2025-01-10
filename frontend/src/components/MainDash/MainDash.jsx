// MainDash.jsx
import React from "react";
import { useParams } from "react-router-dom"; // <-- import useParams
import Cards from "../Cards/Cards";
import Table from "../Table/Table";
import "./MainDash.css";

const MainDash = () => {
  const { company } = useParams(); 
  // 'company' will be "google" / "apple" / "nordvpn" / etc. 
  // If no company param in URL (like just "/"), it's undefined.

  return (
    <div className="MainDash">
      <h1>Dashboard for {company || 'Default'}</h1> {/* Just to visualize */}
      {/* Pass the company param to the child components */}
      <Cards companyName={company} />
      <Table companyName={company} />
    </div>
  );
};

export default MainDash;
