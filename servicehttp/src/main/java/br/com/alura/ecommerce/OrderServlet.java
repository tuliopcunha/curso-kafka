package br.com.alura.ecommerce;

import br.com.alura.ecommerce.dispatcher.KafkaDispatcher;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

public class OrderServlet extends HttpServlet {

    private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();

    @Override
    public void destroy() {
        super.destroy();
        orderDispatcher.close();
    }

    @Override
    public void init(ServletConfig config) throws ServletException {
        super.init(config);
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        try {
            // we are not caring about any security issues
            // we are hot to use http as start point
            var email = req.getParameter("email");
            var amount = new BigDecimal(req.getParameter("amount"));
            var orderId = req.getParameter("uuid");
            var order = new Order(orderId, amount, email);

            try(var database = new OrdersDatabase()){
                if(database.saveNew(order)){
                    orderDispatcher.send("ECOMMERCE_NEW_ORDER", email, new CorrelationId(OrderServlet.class.getSimpleName()), order);

                    System.out.println("New order send successfully");
                    resp.setStatus(HttpServletResponse.SC_OK);
                    resp.getWriter().println("New order send successfully");
                }else{
                    System.out.println("Out order received");
                    resp.setStatus(HttpServletResponse.SC_OK);
                    resp.getWriter().println("Old order received");
                }
            }


        } catch (ExecutionException | InterruptedException | SQLException e) {
            throw new ServletException(e);
        }
    }
}
