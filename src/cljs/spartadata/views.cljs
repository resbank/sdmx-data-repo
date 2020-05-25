(ns spartadata.views
  (:require [re-frame.core :as re-frame]))


(defn gear-svg []
  [:a {:href "#" :on-click (fn [e] (.preventDefault e) (let [style (-> js/document (.getElementById "side-bar") (.-style) )]
                                                         (if (= "block" (.-display style)) 
                                                           (.setProperty style "display" "none")
                                                           (.setProperty style "display" "block"))) )}
   [:svg {:width "1.5em" :height "1.5em" :view-box "0 0 16 16" :fill :current-color :xmlns "http://www.w3.org/2000/svg"}
    [:path {:fill-rule :evenodd :d "M8.837 1.626c-.246-.835-1.428-.835-1.674 0l-.094.319A1.873 1.873 0 014.377 3.06l-.292-.16c-.764-.415-1.6.42-1.184 1.185l.159.292a1.873 1.873 0 01-1.115 2.692l-.319.094c-.835.246-.835 1.428 0 1.674l.319.094a1.873 1.873 0 011.115 2.693l-.16.291c-.415.764.42 1.6 1.185 1.184l.292-.159a1.873 1.873 0 012.692 1.116l.094.318c.246.835 1.428.835 1.674 0l.094-.319a1.873 1.873 0 012.693-1.115l.291.16c.764.415 1.6-.42 1.184-1.185l-.159-.291a1.873 1.873 0 011.116-2.693l.318-.094c.835-.246.835-1.428 0-1.674l-.319-.094a1.873 1.873 0 01-1.115-2.692l.16-.292c.415-.764-.42-1.6-1.185-1.184l-.291.159A1.873 1.873 0 018.93 1.945l-.094-.319zm-2.633-.283c.527-1.79 3.065-1.79 3.592 0l.094.319a.873.873 0 001.255.52l.292-.16c1.64-.892 3.434.901 2.54 2.541l-.159.292a.873.873 0 00.52 1.255l.319.094c1.79.527 1.79 3.065 0 3.592l-.319.094a.873.873 0 00-.52 1.255l.16.292c.893 1.64-.902 3.434-2.541 2.54l-.292-.159a.873.873 0 00-1.255.52l-.094.319c-.527 1.79-3.065 1.79-3.592 0l-.094-.319a.873.873 0 00-1.255-.52l-.292.16c-1.64.893-3.433-.902-2.54-2.541l.159-.292a.873.873 0 00-.52-1.255l-.319-.094c-1.79-.527-1.79-3.065 0-3.592l.319-.094a.873.873 0 00.52-1.255l-.16-.292c-.892-1.64.902-3.433 2.541-2.54l.292.159a.873.873 0 001.255-.52l.094-.319z" :clip-rule :evenodd}]
    [:path {:fill-rule :evenodd :d "M8 5.754a2.246 2.246 0 100 4.492 2.246 2.246 0 000-4.492zM4.754 8a3.246 3.246 0 116.492 0 3.246 3.246 0 01-6.492 0z" :clip-rule :evenodd}]]])

(defn expand [e] 
  (let [acc (.-target e)]
    (-> acc (.-classList) (.toggle "active"))
    (let [style (-> acc (.-nextElementSibling) (.-style))]
      (if (= (.-display style) "block")
        (.setProperty style "display" "none")
        (.setProperty style "display" "block")))))

(defn main-panel []
  (let [name (re-frame/subscribe [:name])]
    (fn []
      [:div#app-body
       [:header 
        [:a {:style {:padding "1rem"}} "Log in"]
        [:a {:style {:justify-self :center :font-size :x-large :padding "1rem"}} "ERD Data Warehouse"]
        [:div {:style {:justify-self :end :padding "1rem"}} [gear-svg]]]
       [:nav 
        [:ul#sparta-tabs 
         [:li.sparta-tab {:class :active} "Query"]
         [:li.sparta-tab "Data"]
         [:li.sparta-tab "Plot"]]]
       [:div#side-bar {:style {:position :fixed :top 60 :right 0 :background-color :white :width "25rem" :height "100%" :display "none" :border "solid black 1px"}}]
       [:main 
        [:h1 "!!!!!!! UNDER CONSTRUCTION !!!!!!!"]
        [:button.accordion.active {:on-click expand} "Select Dataflow"]
        [:div.panel {:style {:display "block"}}
         [:p "Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat."]]
        [:button.accordion {:on-click expand} "Select Dimensions"]
        [:div.panel
         [:p "Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat."]]
        [:button.accordion {:on-click expand} "Export Options"]
        [:div.panel
         [:p "Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat."]]
        [:h2 "Some further information"]
        [:p "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Consequat nisl vel pretium lectus quam id leo in vitae. Varius vel pharetra vel turpis nunc eget. Urna nec tincidunt praesent semper feugiat nibh sed pulvinar proin. Ac tortor vitae purus faucibus ornare suspendisse sed nisi. Sed viverra ipsum nunc aliquet bibendum. Nulla aliquet enim tortor at. Et malesuada fames ac turpis egestas integer. Ornare arcu dui vivamus arcu. Tortor posuere ac ut consequat semper viverra nam. Placerat duis ultricies lacus sed. Iaculis nunc sed augue lacus viverra vitae congue eu. Facilisi morbi tempus iaculis urna. Augue eget arcu dictum varius duis at consectetur lorem. Commodo nulla facilisi nullam vehicula ipsum a arcu. Enim nec dui nunc mattis enim ut tellus. Morbi blandit cursus risus at ultrices mi. Nulla facilisi nullam vehicula ipsum a."]
        [:p "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Consequat nisl vel pretium lectus quam id leo in vitae. Varius vel pharetra vel turpis nunc eget. Urna nec tincidunt praesent semper feugiat nibh sed pulvinar proin. Ac tortor vitae purus faucibus ornare suspendisse sed nisi. Sed viverra ipsum nunc aliquet bibendum. Nulla aliquet enim tortor at. Et malesuada fames ac turpis egestas integer. Ornare arcu dui vivamus arcu. Tortor posuere ac ut consequat semper viverra nam. Placerat duis ultricies lacus sed. Iaculis nunc sed augue lacus viverra vitae congue eu. Facilisi morbi tempus iaculis urna. Augue eget arcu dictum varius duis at consectetur lorem. Commodo nulla facilisi nullam vehicula ipsum a arcu. Enim nec dui nunc mattis enim ut tellus. Morbi blandit cursus risus at ultrices mi. Nulla facilisi nullam vehicula ipsum a."]]
       [:footer 
        [:span {:style {:grid-column "2/3" :font-size :small}} "Web administrator " [:a {:href "mailto:Byron.Botha@resbank.co.za"} "Byron Botha"] ", extension: 4626"]
        [:span {:style {:grid-column "3/4" :font-size :small}} "Part of the Sparta Project"]]])))
