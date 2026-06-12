GROCERY_TREE = {
    "store_type": "grocery",
    "brand_styles": [
        "Field & Vine", "Harvest Crown", "Miller's Best", "Golden Orchard",
        "Prairie Lane", "Bluebird Farms", "Stonebridge", "Vista Verde",
        "Hearthstone", "Morning Glory", "Coastal Catch", "Sunrise Valley",
    ],
    "departments": [
        {"name": "Fresh", "categories": [
            {"category": "Produce", "brand_share": 0.15, "subcategories": [
                {"name": "Fresh Fruit", "price_min": 0.99, "price_max": 9.99,
                 "nouns": ["Apples", "Bananas", "Strawberries", "Blueberries", "Grapes",
                            "Oranges", "Peaches", "Pears", "Mangoes", "Pineapple",
                            "Watermelon", "Raspberries"],
                 "modifiers": ["Organic", "Fresh", "Premium", "Local", "Family Pack", "Snack Size"]},
                {"name": "Fresh Vegetables", "price_min": 0.99, "price_max": 8.99,
                 "nouns": ["Tomatoes", "Carrots", "Broccoli", "Peppers", "Onions",
                            "Potatoes", "Spinach", "Lettuce", "Cucumbers", "Mushrooms",
                            "Zucchini", "Asparagus"],
                 "modifiers": ["Organic", "Fresh", "Premium", "Local", "Family Pack", "Steam-Ready"]},
            ]},
            {"category": "Meat & Seafood", "brand_share": 0.35, "subcategories": [
                {"name": "Beef & Pork", "price_min": 4.99, "price_max": 29.99,
                 "nouns": [
                     "Ground Beef", "Ribeye Steak", "Chuck Roast", "Pork Chops",
                     "Bacon Strips", "Pork Tenderloin", "Beef Brisket", "Sirloin Steak",
                     "Pork Shoulder", "Beef Short Ribs", "Ground Pork", "Flank Steak",
                 ],
                 "modifiers": [
                     "Grass-Fed", "Natural", "Premium", "Lean", "Thick-Cut", "Hormone-Free",
                 ]},
                {"name": "Poultry", "price_min": 3.99, "price_max": 19.99,
                 "nouns": [
                     "Chicken Breast", "Chicken Thighs", "Whole Chicken", "Chicken Wings",
                     "Turkey Breast", "Ground Turkey", "Chicken Drumsticks", "Turkey Cutlets",
                     "Chicken Tenders", "Duck Breast", "Cornish Hen", "Chicken Quarters",
                 ],
                 "modifiers": [
                     "Free-Range", "Antibiotic-Free", "Natural", "Skinless", "Boneless", "Air-Chilled",
                 ]},
                {"name": "Seafood", "price_min": 5.99, "price_max": 34.99,
                 "nouns": [
                     "Atlantic Salmon", "Tilapia Fillets", "Shrimp", "Cod Fillets",
                     "Tuna Steaks", "Catfish Fillets", "Sea Scallops", "Mahi-Mahi",
                     "Halibut Fillets", "Clams", "Crab Legs", "Rainbow Trout",
                 ],
                 "modifiers": [
                     "Wild-Caught", "Fresh", "Sustainably Sourced", "Jumbo", "Frozen-at-Sea", "Skinless",
                 ]},
            ]},
            {"category": "Dairy & Eggs", "brand_share": 0.5, "subcategories": [
                {"name": "Milk & Cream", "price_min": 1.99, "price_max": 7.99,
                 "nouns": [
                     "Whole Milk", "2% Milk", "Skim Milk", "Heavy Cream", "Half and Half",
                     "Buttermilk", "Oat Milk", "Almond Milk", "Soy Milk", "Lactose-Free Milk",
                     "Evaporated Milk", "Condensed Milk",
                 ],
                 "modifiers": [
                     "Organic", "Vitamin D", "Reduced-Fat", "Grass-Fed", "Ultra-Pasteurized", "Farm-Fresh",
                 ]},
                {"name": "Cheese", "price_min": 2.99, "price_max": 14.99,
                 "nouns": [
                     "Cheddar", "Mozzarella", "Colby Jack", "Swiss Slices", "Parmesan",
                     "Pepper Jack", "Provolone", "Gouda", "Cream Cheese", "Brie Wheel",
                     "Feta Crumbles", "Ricotta",
                 ],
                 "modifiers": [
                     "Sharp", "Mild", "Aged", "Sliced", "Shredded", "Block",
                 ]},
                {"name": "Yogurt", "price_min": 0.99, "price_max": 7.99,
                 "nouns": [
                     "Strawberry Yogurt", "Vanilla Yogurt", "Blueberry Yogurt", "Plain Yogurt",
                     "Peach Yogurt", "Mixed Berry Yogurt", "Honey Yogurt", "Coconut Yogurt",
                     "Mango Yogurt", "Cherry Yogurt", "Lemon Yogurt", "Raspberry Yogurt",
                 ],
                 "modifiers": [
                     "Greek", "Low-Fat", "Organic", "Probiotic", "Whole Milk", "Nonfat",
                 ]},
                {"name": "Eggs & Butter", "price_min": 2.49, "price_max": 9.99,
                 "nouns": [
                     "Large Eggs", "Extra-Large Eggs", "Brown Eggs", "Cage-Free Eggs",
                     "Salted Butter", "Unsalted Butter", "Whipped Butter", "Cultured Butter",
                     "Pasture-Raised Eggs", "Omega-3 Eggs", "Medium Eggs",
                 ],
                 "modifiers": [
                     "Organic", "Free-Range", "Grade A", "Farm-Fresh", "Premium", "Value Pack",
                 ]},
            ]},
            {"category": "Bakery", "brand_share": 0.3, "subcategories": [
                {"name": "Bread & Rolls", "price_min": 1.99, "price_max": 8.99,
                 "nouns": [
                     "White Sandwich Bread", "Whole Wheat Bread", "Sourdough Loaf",
                     "Multigrain Bread", "Rye Bread", "Potato Bread", "Brioche Loaf",
                     "Dinner Rolls", "Hamburger Buns", "Hot Dog Buns",
                     "French Baguette", "Ciabatta Rolls",
                 ],
                 "modifiers": [
                     "Artisan", "Fresh-Baked", "Sliced", "Thick-Cut", "Organic", "Hearty",
                 ]},
                {"name": "Sweet Goods", "price_min": 2.99, "price_max": 15.99,
                 "nouns": [
                     "Chocolate Cake", "Glazed Donuts", "Blueberry Muffins", "Cinnamon Rolls",
                     "Croissants", "Brownies", "Lemon Pound Cake", "Apple Fritters",
                     "Chocolate Chip Cookies", "Carrot Cake Slices", "Pecan Pie",
                     "Cheese Danish",
                 ],
                 "modifiers": [
                     "Fresh-Baked", "Jumbo", "Mini", "Assorted", "Double", "Classic",
                 ]},
            ]},
        ]},
        {"name": "Grocery", "categories": [
            {"category": "Pantry", "brand_share": 0.6, "subcategories": [
                {"name": "Pasta & Sauces", "price_min": 0.99, "price_max": 9.99,
                 "nouns": [
                     "Spaghetti", "Penne", "Rotini", "Fettuccine", "Linguine",
                     "Rigatoni", "Farfalle", "Elbow Macaroni", "Marinara Sauce",
                     "Alfredo Sauce", "Tomato Basil Sauce", "Arrabbiata Sauce",
                 ],
                 "modifiers": [
                     "Whole Wheat", "Gluten-Free", "Classic", "Premium", "Family Size", "Organic",
                 ]},
                {"name": "Canned Goods", "price_min": 0.79, "price_max": 5.99,
                 "nouns": [
                     "Diced Tomatoes", "Kidney Beans", "Chickpeas", "Chicken Broth",
                     "Beef Broth", "Corn", "Green Beans", "Cream of Mushroom Soup",
                     "Tomato Paste", "Black Beans", "Tuna in Water", "Coconut Milk",
                 ],
                 "modifiers": [
                     "No-Salt-Added", "Low-Sodium", "Organic", "Classic", "Value Pack", "Premium",
                 ]},
                {"name": "Baking", "price_min": 1.49, "price_max": 12.99,
                 "nouns": [
                     "All-Purpose Flour", "Granulated Sugar", "Brown Sugar", "Powdered Sugar",
                     "Baking Soda", "Baking Powder", "Vanilla Extract", "Cocoa Powder",
                     "Chocolate Chips", "Cornstarch", "Active Dry Yeast", "Bread Flour",
                 ],
                 "modifiers": [
                     "Organic", "Unbleached", "Pure", "Premium", "Fine-Milled", "Natural",
                 ]},
                {"name": "Grains & Rice", "price_min": 1.49, "price_max": 11.99,
                 "nouns": [
                     "Long-Grain White Rice", "Brown Rice", "Jasmine Rice", "Basmati Rice",
                     "Rolled Oats", "Quinoa", "Lentils", "Split Peas", "Farro",
                     "Wild Rice Blend", "Barley", "Bulgur Wheat",
                 ],
                 "modifiers": [
                     "Organic", "Whole Grain", "Quick-Cook", "Premium", "Family Size", "Non-GMO",
                 ]},
                {"name": "Condiments & Oils", "price_min": 1.99, "price_max": 16.99,
                 "nouns": [
                     "Ketchup", "Yellow Mustard", "Mayonnaise", "Soy Sauce",
                     "Olive Oil", "Vegetable Oil", "Apple Cider Vinegar", "Honey",
                     "Hot Sauce", "Worcestershire Sauce", "Ranch Dressing", "Balsamic Vinegar",
                 ],
                 "modifiers": [
                     "Organic", "Extra-Virgin", "Classic", "Reduced-Fat", "Premium", "Bold",
                 ]},
            ]},
            {"category": "Snacks & Beverages", "brand_share": 0.65, "subcategories": [
                {"name": "Chips & Crackers", "price_min": 1.99, "price_max": 6.99,
                 "nouns": [
                     "Potato Chips", "Tortilla Chips", "Pretzels", "Popcorn",
                     "Pita Chips", "Rice Cakes", "Cheddar Crackers", "Multigrain Crackers",
                     "Corn Chips", "Veggie Straws", "Trail Mix Crackers", "Flatbread Crisps",
                 ],
                 "modifiers": [
                     "Sea Salt", "Lightly Salted", "Baked", "Bold", "Kettle-Cooked", "Party Size",
                 ]},
                {"name": "Candy & Chocolate", "price_min": 0.99, "price_max": 9.99,
                 "nouns": [
                     "Milk Chocolate Bar", "Dark Chocolate Bar", "Gummy Bears", "Jelly Beans",
                     "Hard Candy", "Caramel Chews", "Peppermint Patties", "Licorice Twists",
                     "Chocolate Truffles", "Sour Worms", "Taffy Assortment", "Fudge Bites",
                 ],
                 "modifiers": [
                     "Premium", "Assorted", "Mini", "Family Pack", "Holiday", "Dark",
                 ]},
                {"name": "Coffee & Tea", "price_min": 3.99, "price_max": 18.99,
                 "nouns": [
                     "Ground Coffee", "Whole Bean Coffee", "Instant Coffee", "Espresso Pods",
                     "Green Tea Bags", "Black Tea Bags", "Herbal Tea Bags", "Chai Tea Blend",
                     "Cold Brew Coffee", "Decaf Ground Coffee", "Matcha Powder", "Oolong Tea",
                 ],
                 "modifiers": [
                     "Premium", "Organic", "Fair-Trade", "Bold Roast", "Medium Roast", "Light Roast",
                 ]},
                {"name": "Soft Drinks & Water", "price_min": 0.99, "price_max": 9.99,
                 "nouns": [
                     "Cola Soda", "Lemon-Lime Soda", "Ginger Ale", "Root Beer",
                     "Club Soda", "Sparkling Water", "Still Spring Water", "Mineral Water",
                     "Cream Soda", "Orange Soda", "Grape Soda", "Cherry Soda",
                 ],
                 "modifiers": [
                     "Diet", "Zero Sugar", "Caffeine-Free", "12-Pack", "2-Liter", "Natural",
                 ]},
                {"name": "Juice", "price_min": 1.99, "price_max": 8.99,
                 "nouns": [
                     "Orange Juice", "Apple Juice", "Cranberry Juice", "Grape Juice",
                     "Pineapple Juice", "Tomato Juice", "Carrot Juice", "Grapefruit Juice",
                     "Mango Juice", "Pomegranate Juice", "Lemonade", "Fruit Punch",
                 ],
                 "modifiers": [
                     "100% Pure", "Not from Concentrate", "Organic", "No Added Sugar", "Fresh-Pressed", "Low-Acid",
                 ]},
            ]},
            {"category": "Frozen", "brand_share": 0.6, "subcategories": [
                {"name": "Frozen Meals", "price_min": 2.99, "price_max": 12.99,
                 "nouns": [
                     "Chicken Pot Pie", "Macaroni and Cheese", "Beef Lasagna", "Chicken Fried Rice",
                     "Pepperoni Pizza", "Veggie Burrito", "Shrimp Stir-Fry", "Turkey Meatballs",
                     "Cheese Ravioli", "Chicken Alfredo", "Black Bean Enchiladas", "Beef Shepherd's Pie",
                 ],
                 "modifiers": [
                     "Homestyle", "Lean", "Family Size", "Organic", "Gluten-Free", "Classic",
                 ]},
                {"name": "Ice Cream", "price_min": 2.99, "price_max": 9.99,
                 "nouns": [
                     "Vanilla Ice Cream", "Chocolate Ice Cream", "Strawberry Ice Cream",
                     "Mint Chip Ice Cream", "Rocky Road Ice Cream", "Cookie Dough Ice Cream",
                     "Butter Pecan Ice Cream", "Neapolitan Ice Cream", "Coffee Ice Cream",
                     "Caramel Swirl Ice Cream", "Sherbet", "Frozen Yogurt",
                 ],
                 "modifiers": [
                     "Premium", "Light", "No Sugar Added", "Half-Gallon", "Pint", "Organic",
                 ]},
                {"name": "Frozen Vegetables & Fruit", "price_min": 1.49, "price_max": 7.99,
                 "nouns": [
                     "Broccoli Florets", "Mixed Vegetables", "Corn Kernels", "Green Peas",
                     "Edamame", "Stir-Fry Blend", "Cauliflower Florets", "Lima Beans",
                     "Sliced Strawberries", "Mixed Berries", "Mango Chunks", "Spinach",
                 ],
                 "modifiers": [
                     "Steam-in-Bag", "No Added Salt", "Organic", "Family Size", "Petite", "Chopped",
                 ]},
            ]},
            {"category": "Breakfast", "brand_share": 0.6, "subcategories": [
                {"name": "Cereal", "price_min": 2.49, "price_max": 7.99,
                 "nouns": [
                     "Corn Flakes", "Bran Flakes", "Granola", "Toasted Oat Cereal",
                     "Honey Clusters", "Crispy Rice Cereal", "Muesli", "Wheat Flakes",
                     "Puffed Corn Cereal", "Multigrain Flakes", "Frosted Wheat Biscuits",
                     "Cocoa Puffs Cereal",
                 ],
                 "modifiers": [
                     "Whole Grain", "High-Fiber", "Low-Sugar", "Organic", "Family Size", "Gluten-Free",
                 ]},
                {"name": "Breakfast Bars & Oatmeal", "price_min": 1.99, "price_max": 8.99,
                 "nouns": [
                     "Oat & Honey Bar", "Blueberry Granola Bar", "Peanut Butter Bar",
                     "Fruit & Nut Bar", "Chocolate Chip Granola Bar", "Apple Cinnamon Bar",
                     "Instant Oatmeal Packets", "Steel-Cut Oats", "Quick-Cook Oatmeal",
                     "Maple Brown Sugar Oatmeal", "Coconut Almond Bar", "Protein Oat Bar",
                 ],
                 "modifiers": [
                     "Organic", "Gluten-Free", "High-Protein", "No Artificial Colors", "Chewy", "Crunchy",
                 ]},
            ]},
        ]},
        {"name": "Household & Personal Care", "categories": [
            {"category": "Household", "brand_share": 0.55, "subcategories": [
                {"name": "Cleaning", "price_min": 1.99, "price_max": 14.99,
                 "nouns": [
                     "All-Purpose Cleaner", "Dish Soap", "Laundry Detergent", "Fabric Softener",
                     "Bleach", "Glass Cleaner", "Bathroom Cleaner", "Floor Cleaner",
                     "Dryer Sheets", "Sponges", "Scrubbing Pads", "Dishwasher Pods",
                 ],
                 "modifiers": [
                     "Fresh Scent", "Lemon", "Lavender", "Concentrated", "Eco-Friendly", "Heavy-Duty",
                 ]},
                {"name": "Paper & Plastics", "price_min": 1.49, "price_max": 19.99,
                 "nouns": [
                     "Paper Towels", "Bathroom Tissue", "Facial Tissue", "Napkins",
                     "Sandwich Bags", "Quart Bags", "Gallon Bags", "Trash Bags",
                     "Aluminum Foil", "Plastic Wrap", "Parchment Paper", "Wax Bags",
                 ],
                 "modifiers": [
                     "Mega Roll", "Double Roll", "Flex-Seal", "Scented", "Strong", "Ultra-Soft",
                 ]},
            ]},
            {"category": "Personal Care", "brand_share": 0.55, "subcategories": [
                {"name": "Hair & Body", "price_min": 1.99, "price_max": 13.99,
                 "nouns": [
                     "Shampoo", "Conditioner", "Body Wash", "Hand Soap", "Bar Soap",
                     "Shaving Cream", "Deodorant Stick", "Body Lotion", "Face Wash",
                     "Moisturizing Cream", "Dry Shampoo", "Hair Conditioner Mask",
                 ],
                 "modifiers": [
                     "Moisturizing", "Volumizing", "Sensitive Skin", "Clarifying", "Natural", "Fragrance-Free",
                 ]},
                {"name": "Oral Care", "price_min": 1.49, "price_max": 9.99,
                 "nouns": [
                     "Toothpaste", "Soft Toothbrush", "Medium Toothbrush", "Dental Floss",
                     "Mouthwash", "Whitening Strips", "Tongue Scraper", "Interdental Brushes",
                     "Electric Toothbrush Heads", "Water Flosser Replacement Tips",
                     "Sensitive Toothpaste", "Floss Picks",
                 ],
                 "modifiers": [
                     "Whitening", "Cavity Protection", "Mint", "Extra-Soft", "Clinical", "Sensitive",
                 ]},
            ]},
        ]},
    ],
}
