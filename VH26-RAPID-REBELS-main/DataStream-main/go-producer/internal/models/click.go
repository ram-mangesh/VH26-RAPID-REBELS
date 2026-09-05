package models

type ClickAction string

const (
	ClickView    ClickAction = "view"
	ClickAdd     ClickAction = "add_to_cart"
	ClickRemove  ClickAction = "remove_from_cart"
	ClickBuy     ClickAction = "buy"
	ClickSearch  ClickAction = "search"
	ClickFilter  ClickAction = "filter"
	ClickShare   ClickAction = "share"
	ClickWish    ClickAction = "wishlist"
	ClickReview  ClickAction = "review"
	ClickScroll  ClickAction = "scroll"
	ClickHover   ClickAction = "hover"
	ClickBanner  ClickAction = "banner_click"
)

var Pages = []string{
	"/home", "/products", "/product-detail", "/cart", "/checkout",
	"/search", "/category/electronics", "/category/clothing",
	"/deals", "/offers", "/profile", "/orders", "/wishlist",
}

var Devices = []string{
	"mobile_android", "mobile_ios", "desktop_chrome", "desktop_firefox",
	"desktop_safari", "tablet_ipad", "mobile_samsung", "mobile_oneplus",
}

var Browsers = []string{
	"Chrome", "Firefox", "Safari", "Edge", "Opera", "Brave",
}

var Referrers = []string{
	"google_search", "facebook_ad", "instagram_ad", "email_campaign",
	"direct", "twitter", "youtube", "affiliate", "sms", "push_notification",
}

type ClickPayload struct {
	SessionID    string       `json:"session_id"`
	CustomerID   string       `json:"customer_id"`
	Page         string       `json:"page"`
	Action       ClickAction  `json:"action"`
	Device       string       `json:"device"`
	Browser      string       `json:"browser"`
	Referrer     string       `json:"referrer"`
	DurationMs   int          `json:"duration_ms,omitempty"`
	Region       string       `json:"region"`
	ProductID    string       `json:"product_id,omitempty"`
	SearchQuery  string       `json:"search_query,omitempty"`
}
