// Data.js
export const getCompaniesData = async () => {
  try {
    const response = await fetch("http://localhost:8000/companies");
    if (!response.ok) {
      throw new Error("Failed to fetch companies data");
    }
    const data = await response.json();
    // data should be of the form { companies: [...] }
    return data.companies; 
  } catch (error) {
    console.error(error);
    return [];
  }
};

export const fetchDashboardData = async (companyName) => {
  try {
    // Fetch "Main" data
    const mainResponse = await fetch(
      `http://localhost:8000/super-aggregated-postgres-data/${companyName}`
    );
    if (!mainResponse.ok) {
      throw new Error("Failed to fetch main data");
    }
    const mainData = await mainResponse.json();

    // Fetch "Reddit", "Trustpilot", and "YouTube" data
    const platformsResponse = await fetch(
      `http://localhost:8000/aggregated-postgres-data/${companyName}`
    );
    if (!platformsResponse.ok) {
      throw new Error("Failed to fetch platform data");
    }
    const platformsData = await platformsResponse.json();

    // 1) Extract and process the "aggregated_data" for main
    const aggregatedMain = mainData.aggregated_data || [];
    const main = aggregatedMain.map(entry => ({
      date: entry.date,
      score: entry.score,
    }));

    // 2) Extract & process the "aggregated_data" for reddit, trustpilot, youtube
    const aggregatedPlatforms = platformsData.aggregated_data || [];

    // Build an array for each platform
    // We'll *only* keep entries that are not null or undefined 
    // so that the chart can safely show them

    const reddit = aggregatedPlatforms
      .filter(item => item.reddit !== null && item.reddit !== undefined)
      .map(item => ({
        date: item.date,
        score: item.reddit
      }));

    const trustpilot = aggregatedPlatforms
      .filter(item => item.trustpilot !== null && item.trustpilot !== undefined)
      .map(item => ({
        date: item.date,
        score: item.trustpilot
      }));

    const youtube = aggregatedPlatforms
      .filter(item => item.youtube !== null && item.youtube !== undefined)
      .map(item => ({
        date: item.date,
        score: item.youtube
      }));

    // Return all card data
    return {
      main,
      reddit,
      trustpilot,
      youtube,
    };
  } catch (error) {
    console.error("Error fetching dashboard data:", error);
    throw error;
  }
};
