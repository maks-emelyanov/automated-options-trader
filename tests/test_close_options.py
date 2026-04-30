import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from trading.close_options import submit_close_order


class SubmitCloseOrderTests(unittest.TestCase):
    def test_recovers_order_by_client_id_after_internal_submit_error(self):
        req = SimpleNamespace(client_order_id="close-cal-ip-c-34000-20260529-20260501")
        order = SimpleNamespace(id="order-123", status="pending_new")
        trade_client = Mock()
        trade_client.get_order_by_client_id.return_value = order

        with patch("trading.close_options.call_with_retries") as call_with_retries:
            call_with_retries.side_effect = [
                RuntimeError('{"code":50010000,"message":"internal server error occurred"}'),
                order,
            ]

            result = submit_close_order(trade_client, req, underlying="IP")

        self.assertIs(result, order)
        self.assertEqual(call_with_retries.call_count, 2)
        lookup_operation = call_with_retries.call_args_list[1].args[0]
        self.assertIs(lookup_operation(), order)
        trade_client.get_order_by_client_id.assert_called_once_with(req.client_order_id)

    def test_reraises_non_ambiguous_submit_error_without_lookup(self):
        req = SimpleNamespace(client_order_id="close-cal-ip-c-34000-20260529-20260501")
        trade_client = Mock()
        submit_error = RuntimeError("invalid option order")

        with patch("trading.close_options.call_with_retries") as call_with_retries:
            call_with_retries.side_effect = submit_error

            with self.assertRaises(RuntimeError) as raised:
                submit_close_order(trade_client, req, underlying="IP")

        self.assertIs(raised.exception, submit_error)
        self.assertEqual(call_with_retries.call_count, 1)
        trade_client.get_order_by_client_id.assert_not_called()


if __name__ == "__main__":
    unittest.main()
