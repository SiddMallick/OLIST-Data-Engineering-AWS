import './App.css';

import Navbar from './components/navbar';
import OrderPerDays from './components/order_per_days';
import CategorywiseTotalSales from './components/categorywise_total_sales';
import ProductCategorySales from './components/product_category_sales';
import KPIS from './components/kpis';
import CityByDeliveryTimeStats from './components/city_by_delivery_time_stats';

function App() {
  return (
    <div className="App">
      <Navbar />

      <div className="container" style={{marginTop:"20px"}}>
          <div className='row'>
            
                <KPIS/>
           
          </div>
          <div className="row">
            <div className="col">
              <OrderPerDays/>
            </div>
            <div className="col">
              <ProductCategorySales/>
            </div>
          </div>
          <div className="row">
            <div className="col">
              <CategorywiseTotalSales/>
            </div>
          </div>
          <div className="row">
            <div className="col">
              <CityByDeliveryTimeStats/>
            </div>
          </div>
        </div>
    </div>
  );
}

export default App;
